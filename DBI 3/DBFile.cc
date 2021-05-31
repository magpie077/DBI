#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>
#include "HeapFile.h"
#include "SortedFile.h"
#include <string.h>
#include<string>

using namespace std;

DBFile::DBFile (): genericVar(NULL) 
{

}


DBFile::~DBFile () 
{
  delete genericVar;
}


File * DBFile::getFile()
{
  return cur_file;
}


int DBFile::Open (char *fpath) 
{

  fType myftype;
  string metadataFilePath=fpath;
  metadataFilePath=metadataFilePath+".metadata";
  fstream outputFile(metadataFilePath.c_str(), fstream::in);
  metadata tst;
  outputFile.read((char *) & tst, sizeof (metadata));
  myftype = tst.filetype;
  
  if (myftype == heap)
  {
   cout << "Opening Heap DBFIle";
   genericVar = new HeapFile();
   
   return genericVar->Open(fpath);
  }

  else if (myftype == sorted)
  {
   cout << "Opening Sorted DBFIle";
   genericVar = new SortedFile();
   genericVar->sort_order = tst.sorting_order;
   genericVar->runlen = tst.runlength;
   return genericVar->Open(fpath);

  }


}


int DBFile::Create (char *f_path, fType f_type, void *startup) 
{
   
  if (f_type == heap)
  {
    string metadataFilePath=f_path;
    metadataFilePath=metadataFilePath+".metadata";
    fstream outputFile(metadataFilePath.c_str(), fstream::out);
    metadata myMeta;
    myMeta.filetype = heap;
    outputFile.write((char *) &myMeta, sizeof(myMeta));
    outputFile.close();
    genericVar = new HeapFile();
    
    return  genericVar->Create(f_path, f_type, startup);
  }

  else if (f_type == sorted)
  {
    metadata myMeta;
    myMeta.filetype = sorted; 
    mysrt =(strtup*)startup;
    
    myMeta.sorting_order = *(OrderMaker *)(mysrt->ord);

    myMeta.runlength = mysrt->l;   
    
    string metadataFilePath=f_path;
    metadataFilePath=metadataFilePath+".metadata";
    fstream outputFile(metadataFilePath.c_str(), fstream::out);
    outputFile.write((char *) &myMeta, sizeof(metadata));
    cout << "Metadata written" << endl;
    outputFile.close();
    cout << "saving into myvar" << endl;
    genericVar = new SortedFile(); 
    genericVar->sort_order = *(mysrt->ord);
    genericVar->runlen = mysrt->l;

    cout << "Sending to Sorted File" << endl;
    return genericVar->Create(f_path, f_type, startup);
  }

}


void DBFile::MoveFirst () 
{
  genericVar->MoveFirst();
}


void DBFile::Load (Schema &f_schema, char *loadpath) 
{
  genericVar->Load(f_schema,loadpath);
}


void DBFile::Add (Record &rec) 
{
  genericVar->Add(rec);
}


int DBFile::Close () 
{
  return genericVar->Close();
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
  return genericVar->GetNext(fetchme,cnf,literal);
}

int DBFile::GetNext (Record &fetchme) 
{
  return genericVar->GetNext(fetchme);
}


