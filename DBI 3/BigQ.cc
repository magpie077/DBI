#include "BigQ.h"
#include <vector>
#include <string>
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <algorithm>
#include<set>


//-------- Constructor------------
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
	inP = &in;
	opP = &out;
	sortingOrder = &sortorder;
	pthread_t sortThread;
	runLength = runlen;
	fName ="bigqTemp.bin";
	file.Open(0, fName);
	pg_no = 0;
	no_of_runs = 0;
	int success = pthread_create(&sortThread, NULL, &sortingWorker, (void*)this);
}

// This is the fn of RecV class which is used to compare 2 records.
int RecV :: compareRec (const void *r1, const void *r2) 
{
	RecV *rv1 = (RecV *)r1;
	RecV *rv2 = (RecV *)r2;
	ComparisonEngine ce;
	int result = ce.Compare(&(rv1->tmpRecord), &(rv2->tmpRecord), rv2->sortedOrder);
	if(result < 0) 
	{
        return 1;
	}
	else 
	{
		return 0;
	}
}


class recordsCompare {
public:
	int operator() (recordsW *r1, recordsW *r2 ) {
	ComparisonEngine ce;
	int result = ce.Compare( &(r1->newRecord), &(r2->newRecord), r1->sortedOrder );
	if (result < 0)
	 return 1;
	else
	 return 0;
	}
      };

// This is the methods which calls the sortRecs method
void* BigQ:: sortingWorker(void *sortThread)
{
	BigQ *bigQobj = (BigQ *) sortThread;
	bigQobj -> sortRecs();
        
	pthread_exit(NULL);
}

// This is the main method which sorts the records in all the runs and then later
// calls the merge function
void BigQ::sortRecs()
{
	vector<RecV*> recVector;
    RecV *cpyRec;
	Record *rec;
	int pg_cnt=0;
    Page page;
    rec = new Record;
    
	while(inP->Remove(rec))
	{   
        cpyRec = new RecV;
        (cpyRec->tmpRecord).Copy(rec);
        (cpyRec->sortedOrder) = sortingOrder;
		if(!page.Append(rec)) 
		{
		    pg_cnt++;
            if (pg_cnt == runLength)
            {
               	sort(recVector.begin(), recVector.end(), RecV::compareRec);   
               	writeToFile(recVector);
              
               	recVector.clear();
               	pg_cnt = 0;
            }

            else
            {
               	page.EmptyItOut(); 
               	page.Append(rec);
            }
      	}
     	recVector.push_back(cpyRec);
            
	}

	// for writing in the file for the last time (because the run is not complete)
    if(recVector.size() != 0) 
    {
       	sort(recVector.begin(), recVector.end(), RecV::compareRec);
       	writeToFile(recVector);
       	recVector.clear();
    }

    inP->ShutDown();
    cout << "Input pipe is shutting down" <<endl;
    mergeRecs(); 
}

// This method writes all the records in a run in the sorted order
void BigQ :: writeToFile(vector<RecV*> rcVector) 
{

	  
	no_of_runs++;
    Page my_pg;
    runmetaData *rmd = new runmetaData;
    rmd->startPage = pg_no;
    //int cccnt=0;
    vector<RecV*>::iterator startIt = rcVector.begin();
    vector<RecV*>::iterator endIt = rcVector.end();
    while(startIt != endIt) 
    {
    
        if(!my_pg.Append(&((*startIt)->tmpRecord))) 
        { 
           	file.AddPage(&my_pg, pg_no);
           	
           	pg_no++;
           	my_pg.EmptyItOut();
           	my_pg.Append( &((*startIt)->tmpRecord));
           	//cccnt++;
        } 

       	startIt++;

   	}     

	file.AddPage(&my_pg, pg_no);
    my_pg.EmptyItOut();
    rmd->endPage = pg_no; 
    runmetaDataVec.push_back(rmd); 
    pg_no++;
}


void BigQ::mergeRecs()
{	

	int comp_runs = 0; 
    int counter=0;
	vector<pageWrap*> pg_vec; 
	pageWrap *fPage = NULL; 
	int cur_pg_no =  0 ; 
	
	for(int i=1; i<=no_of_runs ;  i++) 
	{	
	   	cur_pg_no = (runmetaDataVec[i-1])->startPage; 
	   	fPage = new pageWrap;
	   	file.GetPage( &(fPage->n_page), cur_pg_no); 
	   	fPage->cur_page = cur_pg_no;
	   	pg_vec.push_back(fPage); 
	}

	multiset<recordsW*, recordsCompare> mergeset; 
	recordsW *tempRec = NULL ; 

	for(int j=0; j<no_of_runs ;  j++)
	{	
	   	tempRec = new recordsW; 
	   	if(((pg_vec[j])->n_page).GetFirst( &(tempRec->newRecord)) != 0) 
	   	{	   
	       	tempRec->runPosition = (j+1);
	       	(tempRec->sortedOrder) = (this->sortingOrder); 
	       	mergeset.insert(tempRec);
	   	}

	   	else 
       	{	
           	cerr<<"First record not found!"<<endl;
           	exit(0);
       	} 
 	}

	int posrun;
	recordsW *tempWrp;
	while( comp_runs < no_of_runs)
	{
		tempWrp = *(mergeset.begin()) ; 
		mergeset.erase(mergeset.begin()); 
		posrun = tempWrp->runPosition; 
		opP->Insert( &(tempWrp->newRecord)); 
	        counter++;
		if((pg_vec[posrun-1]->n_page).GetFirst( &(tempWrp->newRecord) ) == 0) 
	 	{
		  	pg_vec[posrun-1]->cur_page++; 
		  	if(pg_vec[posrun-1]->cur_page <= runmetaDataVec[posrun-1]->endPage ) 
	 	   	{
		    	file.GetPage(&(pg_vec[posrun-1]->n_page), pg_vec[posrun-1]->cur_page);
		     	if( (pg_vec[posrun-1]->n_page).GetFirst( &(tempWrp->newRecord) ) == 0 ) 
		     	{
		      		cerr<<"Empty Page!!"<<endl;
		      		exit(0);
		     	}
				tempWrp->runPosition = posrun; 
				mergeset.insert(tempWrp);
			}	
		   	else 
		   	{
		  		comp_runs++; 
		   	}
	  	}
	  	else
		{
		 	tempWrp->runPosition = posrun; 
		 	mergeset.insert(tempWrp);
		}
	}
	opP->ShutDown(); 
}

BigQ::~BigQ () {
}