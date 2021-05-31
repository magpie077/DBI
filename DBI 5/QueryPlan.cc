#include <cstring>
#include <climits>
#include <string>
#include <algorithm>

#include "Defs.h"
#include "Errors.h"
#include "Stl.h"
#include "QueryPlan.h"
#include "Pipe.h"
#include "RelOp.h"

#define _OUTPUT_SCHEMA__

#define popVector(vel, el1, el2)                \
  QueryNode* el1 = vel.back();                  \
  vel.pop_back();                               \
  QueryNode* el2 = vel.back();                  \
  vel.pop_back();

#define makeNode(pushed, recycler, nodeType, newNode, params)           \
  AndList* pushed;                                                      \
  nodeType* newNode = new nodeType params;                              \
  concatList(recycler, pushed);

#define freeAll(freeList)                                        \
  for (size_t __ii = 0; __ii < freeList.size(); ++__ii) {        \
    --freeList[__ii]->pipe_Id; free(freeList[__ii]);  } // recycler pipe_Ids but do not free children

#define makeAttr(newAttr, name1, type1)                 \
  newAttr.name = name1; newAttr.myType = type1;

#define indent(level) (string(3*(level), ' ') + "-> ")
#define annot(level) (string(3*(level+1), ' ') + "* ")

using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

// from parser
extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;


/**********************************************************************
 * API                                                                *
 **********************************************************************/
QueryPlan::QueryPlan(Statistics* st): root(NULL), outName("STDOUT"), stat(st), used(NULL) {}

void QueryPlan::plan() {
  makeLeafs();  // these nodes read from file
  makeJoins();
  makeSums();
  makeProjects();
  makeDistinct();
  makeWrite();

  // clean up
  swap(boolean, used);
  FATALIF(used, "WHERE clause syntax error.");
}

void QueryPlan::print(std::ostream& os) const {
  root->print(os);
}

void QueryPlan::setOutput(char* out) {
  outName = out;
}

void QueryPlan::execute() {
  outFile = (outName == "STDOUT" ? stdout
    : outName == "NONE" ? NULL
    : fopen(outName.c_str(), "w"));   // closed by query executor
  if (outFile) {
    int numNodes = root->pipe_Id;
    Pipe** pipes = new Pipe*[numNodes];
    RelationalOp** relops = new RelationalOp*[numNodes];
    root->execute(pipes, relops);
    for (int i=0; i<numNodes; ++i)
      relops[i] -> WaitUntilDone();
    for (int i=0; i<numNodes; ++i) {
      delete pipes[i]; delete relops[i];
    }
    delete[] pipes; delete[] relops;
    if (outFile!=stdout) fclose(outFile);
  }
  root->pipe_Id = 0;
  delete root; root = NULL;
  nodes.clear();
}


/**********************************************************************
 * Query optimization                                                 *
 **********************************************************************/
void QueryPlan::makeLeafs() {
  for (TableList* table = tables; table; table = table->next) {
    stat->CopyRel(table->tableName, table->aliasAs);
    makeNode(pushed, used, LeafNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, stat));
    nodes.push_back(newLeaf);
  }
}

void QueryPlan::makeJoins() {
  orderJoins();
  while (nodes.size()>1) {
    popVector(nodes, left, right);
    makeNode(pushed, used, JoinNode, newJoinNode, (boolean, pushed, left, right, stat));
    nodes.push_back(newJoinNode);
  }
  root = nodes.front();
}

void QueryPlan::makeSums() {
  if (groupingAtts) {
    FATALIF (!finalFunction, "Grouping without aggregation functions!");
    FATALIF (distinctAtts, "No dedup after aggregate!");
    if (distinctFunc) root = new DedupNode(root);
    root = new GroupByNode(groupingAtts, finalFunction, root);
  } else if (finalFunction) {
    root = new SumNode(finalFunction, root);
  }
}

void QueryPlan::makeProjects() {
  if (attsToSelect && !finalFunction && !groupingAtts) root = new ProjectNode(attsToSelect, root);
}

void QueryPlan::makeDistinct() {
  if (distinctAtts) root = new DedupNode(root);
}

void QueryPlan::makeWrite() {
  root = new WriteNode(outFile, root);
}

void QueryPlan::orderJoins() {
  std::vector<QueryNode*> operands(nodes);
  sort(operands.begin(), operands.end());
  int minCost = INT_MAX, cost;
  do {           // traverse all possible permutations
    if ((cost=evalOrder(operands, *stat, minCost))<minCost && cost>0) {
      minCost = cost; nodes = operands; 
    }
  } while (next_permutation(operands.begin(), operands.end()));
}

int QueryPlan::evalOrder(std::vector<QueryNode*> operands, Statistics st, int bestFound) {  // intentional copy
  std::vector<JoinNode*> freeList;  // all new nodes made in this simulation; need to be freed
  AndList* recycler = NULL;         // AndList needs recycling
  while (operands.size()>1) {       // simulate join
    popVector(operands, left, right);
    makeNode(pushed, recycler, JoinNode, newJoinNode, (boolean, pushed, left, right, &st));
    operands.push_back(newJoinNode);
    freeList.push_back(newJoinNode);
    if (newJoinNode->estimate<=0 || newJoinNode->cost>bestFound) break;  // branch and bound
  }
  int cost = operands.back()->cost;
  freeAll(freeList);
  concatList(boolean, recycler);   // put the AndLists back for future use
  return operands.back()->estimate<0 ? -1 : cost;
}

void QueryPlan::concatList(AndList*& left, AndList*& right) {
  if (!left) { swap(left, right); return; }
  AndList *pre = left, *cur = left->rightAnd;
  for (; cur; pre = cur, cur = cur->rightAnd);
  pre->rightAnd = right;
  right = NULL;
}

/**********************************************************************
 * Node construction                                                  *
 **********************************************************************/
int QueryNode::pipe_Id = 0;

QueryNode::QueryNode(const std::string& op, Schema* out, Statistics* st):
  op_Name(op), out_Schema(out), num_Rels(0), estimate(0), cost(0), stat(st), p_out(pipe_Id++) {}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rName, Statistics* st):
  op_Name(op), out_Schema(out), num_Rels(0), estimate(0), cost(0), stat(st), p_out(pipe_Id++) {
  if (rName) relNames[num_Rels++] = strdup(rName);
}

QueryNode::QueryNode(const std::string& op, Schema* out, char* rNames[], size_t num, Statistics* st):
  op_Name(op), out_Schema(out), num_Rels(0), estimate(0), cost(0), stat(st), p_out(pipe_Id++) {
  for (; num_Rels<num; ++num_Rels)
    relNames[num_Rels] = strdup(rNames[num_Rels]);
}

QueryNode::~QueryNode() {
  //delete out_Schema;
  for (size_t i=0; i<num_Rels; ++i)
    delete[] relNames[i];
}

AndList* QueryNode::pushSelection(AndList*& alist, Schema* target) {
  AndList header; header.rightAnd = alist;  // make a list header to
  // avoid handling special cases deleting the first list element
  AndList *cur = alist, *pre = &header, *result = NULL;
  for (; cur; cur = pre->rightAnd)
    if (containedIn(cur->left, target)) {   // should push
      pre->rightAnd = cur->rightAnd;
      cur->rightAnd = result;        // *move* the node to the result list
      result = cur;        // prepend the new node to result list
    } else pre = cur;
  alist = header.rightAnd;  // special case: first element moved
  return result;
}

bool QueryNode::containedIn(OrList* ors, Schema* target) {
  for (; ors; ors=ors->rightOr)
    if (!containedIn(ors->left, target)) return false;
  return true;
}

bool QueryNode::containedIn(ComparisonOp* cmp, Schema* target) {
  Operand *left = cmp->left, *right = cmp->right;
  return (left->code!=NAME || target->Find(left->value)!=-1) &&
         (right->code!=NAME || target->Find(right->value)!=-1);
}

LeafNode::LeafNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st):
  QueryNode("Select File", new Schema(catalog_path, relName, alias), relName, st), opened(false) {
  pushed = pushSelection(boolean, out_Schema);
  estimate = stat->ApplyEstimate(pushed, relNames, num_Rels);
  selOp.GrowFromParseTree(pushed, out_Schema, literal);
}

UnaryNode::UnaryNode(const std::string& op_Name, Schema* out, QueryNode* c, Statistics* st):
  QueryNode (op_Name, out, c->relNames, c->num_Rels, st), child(c), pin(c->p_out) {}

BinaryNode::BinaryNode(const std::string& op_Name, QueryNode* l, QueryNode* r, Statistics* st):
  QueryNode (op_Name, new Schema(*l->out_Schema, *r->out_Schema), st),
  left(l), right(r), pleft(left->p_out), pright(right->p_out) {
  for (size_t i=0; i<l->num_Rels;)
    relNames[num_Rels++] = strdup(l->relNames[i++]);
  for (size_t j=0; j<r->num_Rels;)
    relNames[num_Rels++] = strdup(r->relNames[j++]);
}

ProjectNode::ProjectNode(NameList* atts, QueryNode* c):
  UnaryNode("Project", NULL, c, NULL), numAttsIn(c->out_Schema->GetNumAtts()), numAttsOut(0) {
  Schema* cSchema = c->out_Schema;
  Attribute resultAtts[MAX_ATTS];
  FATALIF (cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  for (; atts; atts=atts->next, numAttsOut++) {
    FATALIF ((keepMe[numAttsOut]=cSchema->Find(atts->name))==-1,
             "Projecting non-existing attribute.");
    makeAttr(resultAtts[numAttsOut], atts->name, cSchema->FindType(atts->name));
  }
  out_Schema = new Schema ("", numAttsOut, resultAtts);
}

DedupNode::DedupNode(QueryNode* c):
		UnaryNode("Deduplication", c->out_Schema, c, NULL), dedupOrder(c->out_Schema) {}

JoinNode::JoinNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st):
  BinaryNode("Join", l, r, st) {
  pushed = pushSelection(boolean, out_Schema);
  estimate = stat->ApplyEstimate(pushed, relNames, num_Rels);
  cost = l->cost + estimate + r->cost;
  selOp.GrowFromParseTree(pushed, l->out_Schema, r->out_Schema, literal);
}

SumNode::SumNode(FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Sum", resultSchema(parseTree, c), c, NULL) {
  f.GrowFromParseTree (parseTree, *c->out_Schema);
}

Schema* SumNode::resultSchema(FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  fun.GrowFromParseTree (parseTree, *c->out_Schema);
  return new Schema ("", 1, atts[fun.resultType()]);
}

GroupByNode::GroupByNode(NameList* gAtts, FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Group by", resultSchema(gAtts, parseTree, c), c, NULL) {
  grpOrder.growFromParseTree(gAtts, c->out_Schema);
  f.GrowFromParseTree (parseTree, *c->out_Schema);
}

Schema* GroupByNode::resultSchema(NameList* gAtts, FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  Schema* cSchema = c->out_Schema;
  fun.GrowFromParseTree (parseTree, *cSchema);
  Attribute resultAtts[MAX_ATTS];
  FATALIF (1+cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  makeAttr(resultAtts[0], "sum", fun.resultType());
  int numAtts = 1;
  for (; gAtts; gAtts=gAtts->next, numAtts++) {
    FATALIF (cSchema->Find(gAtts->name)==-1, "Grouping by non-existing attribute.");
    makeAttr(resultAtts[numAtts], gAtts->name, cSchema->FindType(gAtts->name));
  }
  return new Schema ("", numAtts, resultAtts);
}

WriteNode::WriteNode(FILE*& out, QueryNode* c):
  UnaryNode("WriteOut", new Schema(*c->out_Schema), c, NULL), outFile(out) {}


/**********************************************************************
 * Query execution                                                    *
 **********************************************************************/
void LeafNode::execute(Pipe** pipes, RelationalOp** relops) {
  std::string dbName = std::string(relNames[0]) + ".bin";
  dbf.Open((char*)dbName.c_str()); opened = true;
  SelectFile* sf = new SelectFile();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = sf;
  sf -> Run(dbf, *pipes[p_out], selOp, literal);
}

void ProjectNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  Project* p = new Project();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = p;
  p -> Run(*pipes[pin], *pipes[p_out], keepMe, numAttsIn, numAttsOut);
}

void DedupNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  DuplicateRemoval* dedup = new DuplicateRemoval();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = dedup;
  dedup -> Run(*pipes[pin], *pipes[p_out], *out_Schema);
}

void SumNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  Sum* s = new Sum();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = s;
  s -> Run(*pipes[pin], *pipes[p_out], f);
}

void GroupByNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  GroupBy* grp = new GroupBy();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = grp;
  grp -> Run(*pipes[pin], *pipes[p_out], grpOrder, f);
}

void JoinNode::execute(Pipe** pipes, RelationalOp** relops) {
  left -> execute(pipes, relops); right -> execute(pipes, relops);
  Join* j = new Join();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = j;
  j -> Run(*pipes[pleft], *pipes[pright], *pipes[p_out], selOp, literal);
}

void WriteNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  WriteOut* w = new WriteOut();
  pipes[p_out] = new Pipe(PIPE_SIZE);
  relops[p_out] = w;
  w -> Run(*pipes[pin], outFile, *out_Schema);
}

/**********************************************************************
 * Print utilities                                                    *
 **********************************************************************/
void QueryNode::print(std::ostream& os, size_t level) const {
  printOperator(os, level);
  printAnnot(os, level);
  printSchema(os, level);
  printPipe(os, level);
  printChildren(os, level);
}

void QueryNode::printOperator(std::ostream& os, size_t level) const {
  os << indent(level) << op_Name << ": ";
}

void QueryNode::printSchema(std::ostream& os, size_t level) const {
#ifdef _OUTPUT_SCHEMA__
  os << annot(level) << "Output schema:" << endl;
  out_Schema->print(os);
#endif
}

void LeafNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << p_out << endl;
}

void UnaryNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << p_out << endl;
  os << annot(level) << "Input pipe: " << pin << endl;
}

void BinaryNode::printPipe(std::ostream& os, size_t level) const {
  os << annot(level) << "Output pipe: " << p_out << endl;
  os << annot(level) << "Input pipe: " << pleft << ", " << pright << endl;
}

void LeafNode::printOperator(std::ostream& os, size_t level) const {
  os << indent(level) << "Select from " << relNames[0] << ": ";
}

void LeafNode::printAnnot(std::ostream& os, size_t level) const {
  selOp.Print(); 
}

void ProjectNode::printAnnot(std::ostream& os, size_t level) const {
  os << keepMe[0];
  for (size_t i=1; i<numAttsOut; ++i) os << ',' << keepMe[i];
  os << endl;
  os << annot(level) << numAttsIn << " input attributes; " << numAttsOut << " output attributes" << endl;
}

void JoinNode::printAnnot(std::ostream& os, size_t level) const {
  selOp.Print();
  os << annot(level) << "Estimate = " << estimate << ", Cost = " << cost << endl;
}

void SumNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "Function: "; (const_cast<Function*>(&f))->Print();
}

void GroupByNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "OrderMaker: "; (const_cast<OrderMaker*>(&grpOrder))->Print();
  os << annot(level) << "Function: "; (const_cast<Function*>(&f))->Print();
}

void WriteNode::printAnnot(std::ostream& os, size_t level) const {
  os << annot(level) << "Output to " << outFile << endl;
}

