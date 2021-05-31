#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "Defs.h"
#include <fstream>
#include <iostream>
#include "Pipe.h"
#include "GenericDBFile.h"
#include "BigQ.h"

using namespace std;

SortedFile::SortedFile () 
{

}

SortedFile::~SortedFile ()
{

}

void SortedFile::SwitchMode(mode change)
{
	if(status == change)
	{
		return;
	}
	else
	{
		if(change == R)
		{
			f_ptr.AddPage(&p, pageNumber);
			FixDirtyFile();
			status = R;
			return;
		}
		else
		{
			p.EmptyItOut();
			pageNumber = f_ptr.GetLength()-2;
			f_ptr.GetPage(&p,pageNumber);
			status = W;
			return;
		}
	}	
} 


void *produce (void *arg) 
{
	util *my_prod = (util *) arg;
	Record rec;
	int cntr = 0;
	if (my_prod->ldpath_ptr != NULL) 
	{
		
		FILE *tableFile = fopen(my_prod->ldpath_ptr, "r");
		while(rec.SuckNextRecord(my_prod->sch_ptr, tableFile))
		{
			cntr += 1;
			my_prod->pipe_ptr->Insert (&rec);
		}
	}
	else
	{
		int pg_no = 0;
		Page pg1;
		my_prod->f_ptr->GetPage(&pg1,pg_no);
		while(true) {
			if(!pg1.GetFirst(&rec))
			{
				if(pg_no < my_prod->f_ptr->GetLength() -2)
				{
					
					pg_no++;
					my_prod->f_ptr->GetPage(&pg1,pg_no);
					if(!pg1.GetFirst(&rec))
					{
						break;
					}
				}
				else
				{
					break;
				}
			}

		my_prod->pipe_ptr->Insert (&rec);
		cntr++;
		}
	}
	
	my_prod->pipe_ptr->ShutDown ();
}

void *consume (void *arg) 
{

	util *t = (util *) arg;
	Record rec;
        int cnt =0;
        Page p1;
        int pg_no =0;
	while (t->pipe_ptr->Remove (&rec)) 
	{
		cnt++;
		if (!p1.Append(&rec))
		{
			t->f_ptr->AddPage(&p1,pg_no);
			p1.EmptyItOut();
			p1.Append(&rec);
			pg_no++;
		}
	}
	t->f_ptr->AddPage(&p1,pg_no);
	p1.EmptyItOut();
}


void SortedFile::FixDirtyFile() 
{
    int buff_size = 100;
    Pipe input (buff_size);
    Pipe output (buff_size);
    pthread_t thread_1;
    util ip = {&input, &sort_order,NULL, &f_ptr, NULL  };
    pthread_create (&thread_1, NULL, produce, (void *)&ip);
    pthread_t thread_2;
    util op = {&output, &sort_order,NULL, &f_ptr, NULL };
    pthread_create (&thread_2, NULL, consume, (void *)&op);
    BigQ bq (input, output, sort_order, runlen);
    pthread_join (thread_1, NULL);
    pthread_join (thread_2, NULL);


}

int SortedFile::Create (char *f_path, fType f_type, void *startup) 
{

	fstatus.open(f_path);
	f_ptr.Open(0,f_path);
   	f_ptr.AddPage(&p,0);
   	pageNumber=0;
    status = W;
   	pageCount=1;
                
	return 1;
}

void SortedFile::Load (Schema &f_schema, char *loadpath) 
{
        
   
    SwitchMode(W);
	int buff_size = 100; 
    Pipe input (buff_size);
    Pipe output (buff_size);
    pthread_t thread_1;
    util ip = {&input, &sort_order,&f_schema, &f_ptr, loadpath  };
    pthread_create (&thread_1, NULL, produce, (void *)&ip);
    pthread_t thread_2;
    util op = {&output, &sort_order,&f_schema, &f_ptr, loadpath };
    pthread_create (&thread_2, NULL, consume, (void *)&op);
    BigQ bq (input, output, sort_order, runlen);
    pthread_join (thread_1, NULL);
    pthread_join (thread_2, NULL);
	status = R;	

}

int SortedFile::Open(char *f_path)
{
	p.EmptyItOut();
	f_ptr.Open(1,f_path);
	status=R;
	pageCount = f_ptr.GetLength();
	MoveFirst();
return 1;
}

void SortedFile::MoveFirst ()
{
	SwitchMode(R);
	p.EmptyItOut();
	pageNumber = 0;
	f_ptr.GetPage(&p,pageNumber);

}

void SortedFile::Add (Record &rec)
{

	SwitchMode(W);
	if(!p.Append(&rec))
	{

		f_ptr.AddPage(&p, pageNumber);
		p.EmptyItOut();
		pageNumber++;pageCount++;
		p.Append(&rec);
	}	
}

int SortedFile::Close ()
{
    SwitchMode(R);
	p.EmptyItOut();
	f_ptr.Close();
return 1;

}



int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
	SwitchMode(R);
    ComparisonEngine comp;
	while(GetNext(fetchme))
	{
		if (comp.Compare (&fetchme, &literal, &cnf))
		{
			return 1;
		}		
	}
	return 0;
}


int SortedFile::GetNext (Record &fetchme) 
{

	SwitchMode(R);
	if(p.GetFirst(&fetchme))
	{
		return 1;
	}
	else
	{
		if(pageNumber < pageCount - 2)
		{
			pageNumber++;
			f_ptr.GetPage(&p,pageNumber);
			if(GetNext(fetchme))
			{
				return 1;
			}	
		}
	return 0;
	}	
}


