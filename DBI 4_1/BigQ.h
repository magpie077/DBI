#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <string>
#include <algorithm>
using namespace std;

class recordsW {
public:

    Record newRecord;
    int runPosition;
    OrderMaker *sortedOrder;

};


//---------- Record Vector class--------------
// It holds the object of Record class, pointer to the Ordermaker and a compare fn to compare records
class RecV {
public:
        static int compareRec(const void *r1, const void *r2);
        Record tmpRecord;
        OrderMaker *sortedOrder;
};

class runmetaData 
{
public:
    int startPage, endPage;
};

class BigQ {

Pipe *inP, *opP;
OrderMaker *sortingOrder;
int runLength;

public:


       static void* sortingWorker(void* threadid);


        void writeToFile(vector<RecV*> rcVector);
        void sortRecs();
        void mergeRecs();
        vector<pair<int,int> > pageBegin;
        vector<runmetaData*> runmetaDataVec;
         
        File file;
        char *fName;
        int pg_no, no_of_runs;
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

class pageWrap {
public:
Page n_page; 
int cur_page; 
};
#endif