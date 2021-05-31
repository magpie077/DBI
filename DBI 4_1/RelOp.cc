#include "RelOp.h"

using namespace std;

//--------------------   SELECT FILE OPERATION   --------------------

void *SetupThreadSelectFile(void *arg)
{
	((SelectFile *)arg)->Operation_SelectFile();
}

void SelectFile :: Operation_SelectFile()
{
	Record temp;
	dbfile->MoveFirst();
	while(dbfile->GetNext(temp, *myCNF, *rec) == 1)
		out->Insert(&temp);
	out->ShutDown();
}

void SelectFile :: Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->dbfile = &inFile;
	this->out = &outPipe;
	this->myCNF = &selOp;
	this->rec = &literal;
	pthread_create(&myThread, NULL, SetupThreadSelectFile, (void *)this);  
}

void SelectFile :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void SelectFile :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------   SELECT PIPE OPERATION   --------------------
		
void *SetupThreadSelectPipe(void *arg)
{
	((SelectPipe *)arg)->Operation_SelectPipe();
}

void SelectPipe :: Operation_SelectPipe()
{
	Record temp;
	ComparisonEngine comp;
	while(in->Remove(&temp) == 1)
	{
		if(comp.Compare(&temp, rec, myCNF) == 1)
			out->Insert(&temp);
	}
	out->ShutDown();
}

void SelectPipe :: Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->myCNF = &selOp;
	this->rec = &literal;
	pthread_create(&myThread, NULL, SetupThreadSelectPipe, (void *)this); 
}

void SelectPipe :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void SelectPipe :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------   PROJECT OPERATION   --------------------

void *SetupThreadProject(void *arg)
{
	((Project *)arg)->Operation_Project();
}

void Project :: Operation_Project()
{
	Record temp;
	while(in->Remove(&temp) == 1)
	{
		temp.Project(keepAtts, noAttsOut, noAttsIn);
		out->Insert(&temp);
	}
	in->ShutDown();
	out->ShutDown();
}

void Project ::  Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->keepAtts = keepMe;
	this->noAttsIn = numAttsInput;
	this->noAttsOut = numAttsOutput;
	pthread_create(&myThread, NULL, SetupThreadProject, (void *)this);
}

void Project :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void Project :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------   WRITE OUT OPERATION   --------------------

void *SetupThreadWriteOut(void *arg)
{
	((WriteOut *)arg)->Operation_WriteOut();
}

void WriteOut :: Operation_WriteOut()
{
	Record temp;
	while(in->Remove(&temp) == 1)
		temp.WriteRecordsToFile(schema, file);
	fclose(file);
	in->ShutDown();
}

void WriteOut :: Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	this->in = &inPipe;
	this->file = outFile;
	this->schema = &mySchema;
	pthread_create(&myThread, NULL, SetupThreadWriteOut, (void *)this);
}

void WriteOut :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void WriteOut :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------   DUPLICATE REMOVAL OPERATION   --------------------

void *SetupThreadDuplicateRemoval(void *arg)
{
	((DuplicateRemoval *)arg)->Operation_DuplicateRemoval();
}

void DuplicateRemoval :: Operation_DuplicateRemoval()
{
	Record temp, next;
	OrderMaker *sortorder = new OrderMaker(schema);
	Pipe *pipe = new Pipe(100);
	BigQ bigq(*in, *pipe, *sortorder, 10);
	ComparisonEngine comp;
	pipe->Remove(&next);
	temp.Copy(&next);
	out->Insert(&next);
	while(pipe->Remove(&next) == 1)
	{
		if(comp.Compare(&temp, &next, sortorder) != 0)
		{
			temp.Copy(&next);
			out->Insert(&next);
		}
	}
	in->ShutDown();
	out->ShutDown();
	delete pipe;	
}

void DuplicateRemoval :: Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->schema = &mySchema;
	pthread_create(&myThread, NULL, SetupThreadDuplicateRemoval, (void *)this);
}

void DuplicateRemoval :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void DuplicateRemoval :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------   SUM OPERATION   --------------------

void *SetupThreadSum(void *arg)
{
	((Sum *)arg)->Operation_Sum();
}

void Sum :: Operation_Sum()
{
	Record temp, groupSum;
	Type type;
	int sumIntRec, sumIntTotal = 0;
	double sumDblRec, sumDblTotal = 0;
	while(in->Remove(&groupSum) == 1)
	{
		compute->Apply(groupSum, sumIntRec, sumDblRec);
		if(compute->returnsInt == 1)
		{
			type = Int;
			sumIntTotal = sumIntTotal + sumIntRec;
		}
		else
		{
			type = Double;
			sumDblTotal = sumDblTotal + sumDblRec;
		}
	}
	temp.CreateNewRecord(type, sumIntTotal, sumDblTotal);
	out->Insert(&temp);
	out->ShutDown();
}

void Sum :: Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->compute = &computeMe;
	pthread_create(&myThread, NULL, SetupThreadSum, (void *)this);
}

void Sum :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void Sum :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------  GROUPBY OPERATION   --------------------

void *SetupThreadGroupBy(void *arg)
{
	((GroupBy *)arg)->Operation_GroupBy();
}

void GroupBy :: Operation_GroupBy()
{
	Record recArr[2];
	Record *start = NULL, *end = NULL;
	Record *temp = new Record;
	Record *dump = new Record;
	Type type;
	Pipe *pipe = new Pipe(100);
	BigQ bigq(*in, *pipe, *sortorder, 10);
	ComparisonEngine comp;
	int arrbound = (sortorder->numAtts)+1;
	int groupId = 0, sumIntTotal = 0, sumIntRec, attsArr[arrbound]; 
	double sumDblTotal=0, sumDblRec;
	
	attsArr[0] = 0;
	for(int i = 1; i < arrbound; i++)
		attsArr[i] = sortorder->whichAtts[i-1];
	
	while(pipe->Remove(&recArr[groupId%2]) == 1)
	{
		start = end;
		end = &recArr[groupId%2];
		if(start != NULL && end != NULL)
		{
			if(comp.Compare(start, end, sortorder) != 0)
			{
				compute->Apply(*start, sumIntRec, sumDblRec);
				if(compute->returnsInt == 1)
				{
					type = Int;
					sumIntTotal = sumIntTotal + sumIntRec;
				}
				else
				{
					type = Double;
					sumDblTotal = sumDblTotal + sumDblRec;
				}
				int startint = ((int *)start->bits)[1]/sizeof(int) - 1;
				dump->CreateNewRecord(type, sumIntTotal, sumDblTotal);
				temp->MergeRecords(dump, start, 1, startint, attsArr, arrbound, 1);
				out->Insert(temp);
				sumIntTotal = 0;
				sumDblTotal = 0;
			}
			else
			{
				compute->Apply(*start, sumIntRec, sumDblRec);
				if(compute->returnsInt == 1)
				{
					type = Int;
					sumIntTotal = sumIntTotal + sumIntRec;
				}
				else
				{
					type = Double;
					sumDblTotal = sumDblTotal + sumDblRec;
				}
			}
		}
		groupId++;
	}
	
	compute->Apply(*end, sumIntRec, sumDblRec);
	if(compute->returnsInt == 1)
	{
		type = Int;
		sumIntTotal = sumIntTotal + sumIntRec;
	}
	else
	{
		type = Double;
		sumDblTotal = sumDblTotal + sumDblRec;
	}
	int startint = ((int *)start->bits)[1]/sizeof(int) - 1;
	dump->CreateNewRecord(type, sumIntTotal, sumDblTotal);
	temp->MergeRecords(dump, end, 1, startint, attsArr, arrbound, 1);
	out->Insert(temp);
	
	out->ShutDown();
}

void GroupBy :: Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	this->in = &inPipe;
	this->out = &outPipe;
	this->sortorder = &groupAtts;
	this->compute = &computeMe;
	pthread_create(&myThread, NULL, SetupThreadGroupBy, (void *)this);
}

void GroupBy :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void GroupBy :: Use_n_Pages(int n)
{
	this->runlen = n;
}


//--------------------   JOIN OPERATION   ----------------------

void *SetupThreadJoin(void *arg)
{
	((Join *)arg)->Operation_Join();
}

void Join :: Operation_Join()
{
	int leftint, rightint;
	OrderMaker ls_order, rs_order;
	Record *recleft = new Record();
	Record *recright = new Record();
	Record *recjoin = new Record();
	ComparisonEngine comp;
	Pipe *l_pipe = new Pipe(100);
	Pipe *r_pipe = new Pipe(100);
	vector<Record *> v_left, v_right;
	
	if(myCNF->GetSortOrders(ls_order, rs_order))
	{
		BigQ bigqleft(*in_L, *l_pipe, ls_order, 10);
		BigQ bigqright(*in_R, *r_pipe, rs_order, 10);
		l_pipe->Remove(recleft);
		r_pipe->Remove(recright);
		bool l_flag, r_flag = false;
		leftint = ((int *) recleft->bits)[1] / sizeof(int) -1; 
		rightint = ((int *) recright->bits)[1] / sizeof(int) -1; 
		int attsArr[leftint + rightint];
		for(int i=0; i<leftint; i++)
			attsArr[i] = i;
		for(int i=0; i<rightint; i++)
			attsArr[leftint+i] = i;
		
		while(true)
		{
			if(comp.Compare(recleft, &ls_order, recright, &rs_order) < 0)
			{
				if(l_pipe->Remove(recleft) != 1)
				break;
			}
			else if(comp.Compare(recleft, &ls_order, recright, &rs_order) > 0)
			{
				if(r_pipe->Remove(recright) != 1)
					break;
			}
			else
			{
				l_flag = false;
				r_flag = false;
				
				while(true)
				{
					Record *dummy = new Record;
					dummy->Consume(recleft);
					v_left.push_back(dummy);
					if(l_pipe->Remove(recleft) != 1)
					{
						l_flag = true;
						break;
					}
					if(comp.Compare(dummy, recleft, &ls_order) != 0)
						break;
				}
				while(true)
				{
					Record *dummy = new Record;
					dummy->Consume(recright);
					v_right.push_back(dummy);
					if(r_pipe->Remove(recright) != 1)
					{
						r_flag = true;
						break;
					}
					if(comp.Compare(dummy, recright, &rs_order) != 0)
						break;
				}
				
				for(int i = 0; i < v_left.size(); i++)
				{
					for(int j = 0; j < v_right.size(); j++)
					{
						recjoin->MergeRecords(v_left.at(i), v_right.at(j), leftint, rightint, attsArr, leftint+rightint, leftint);
						out->Insert(recjoin);
					}
				}
				v_left.clear();
				v_right.clear();
				
				if(l_flag || r_flag)
					break;
			}
		}
	}
	else
	{
		while(in_R->Remove(recright) == 1)
		{
			Record tempRec;
			tempRec.Consume(recright);
			v_right.push_back(&tempRec);
		}
		in_L->Remove(recleft);
		leftint = ((int *) recleft->bits)[1] / sizeof(int) -1; 
		rightint = ((int *) v_right.at(0)->bits)[1] / sizeof(int) -1;
		
		int attsArr[leftint+rightint];
		for(int i=0; i<leftint; i++)
			attsArr[i] = i;
		for(int i=0; i<rightint; i++)
			attsArr[leftint+i] = i;
		while(in_L->Remove(recleft) == 1)
		{
			for(int j = 0; j < v_right.size(); j++)
			{
				recjoin->MergeRecords(recleft, v_right.at(j), leftint, rightint, attsArr, leftint+rightint, leftint);
				out->Insert(recjoin);
			}
		}
	}
	out->ShutDown();
}

void Join :: Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->in_L = &inPipeL;
	this->in_R = &inPipeR;
	this->out = &outPipe;
	this->myCNF = &selOp;
	this->rec = &literal;
	pthread_create(&myThread, NULL, SetupThreadJoin, (void *)this);
}

void Join :: WaitUntilDone()
{
	pthread_join(myThread, NULL);
}

void Join :: Use_n_Pages(int n)
{
	this->runlen = n;
}