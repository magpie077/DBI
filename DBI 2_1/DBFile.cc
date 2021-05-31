#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include<string>
using namespace std;

DBFile::DBFile () 
{
	cur_file 	= new File;
	cur_page_w 	= new Page;
	cur_page_r	= new Page;
	file_name 	= NULL;
	page_no	= 0;
}

DBFile::~DBFile() 
{
	delete(cur_file);
	delete(cur_page_w);
	delete(cur_page_r);
}

File * DBFile::getFile()
{
	//cout<<"HH";
	return cur_file;
} 
// Return the file length (in number of pages)
int DBFile::Create (char *f_path, fType f_type, void *startup) 
{
	
	file_name  = strdup(f_path);
	cur_file->Open(0, f_path);


	return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) 
{
	Record rec;
	FILE *file = fopen (loadpath, "r");
	while (rec.SuckNextRecord(&f_schema, file) == 1) 
	{
			this->Add(rec);
	}
}

int DBFile::Open (char *f_path) 
{
	cur_file->Open(1, f_path);
		return 1;

}

void DBFile::MoveFirst () 
{
	cur_page_w->EmptyItOut();
	page_no = 0;
}

int DBFile::Close () 
{
	if(cur_page_r->GetNumRecords() != 0) 
	{
		cur_file->AddPage(cur_page_r, cur_file->GetLength());
		cur_page_r->EmptyItOut();
	}

	if(cur_file->Close() >= 0 )
		return 1;
	else
		return 0;
}

void DBFile::Add (Record &rec) 
{
	int r;

	r = cur_page_r->Append(&rec);
	if(!r) 
	{
		cur_file->AddPage(cur_page_r, cur_file->GetLength());
		cur_page_r->EmptyItOut();
		cur_page_r->Append(&rec);
	}
	
}



int DBFile::GetNext (Record &fetchme) 
{

	if(cur_page_w->GetFirst(&fetchme) == 0)
	{

		if(page_no <= cur_file->GetLength() - 2) 
		{
			cur_file->GetPage(cur_page_w, page_no);
			page_no = page_no + 2;
		}
		else 
		{
			if(cur_page_r->GetNumRecords()) 
			{
				
				if(cur_page_r->GetNumRecords() != 0) 
				{
					cur_file->AddPage(cur_page_r, cur_file->GetLength());
					cur_page_r->EmptyItOut();
				}
				cur_file->GetPage(cur_page_w, page_no);
				page_no = page_no + 2;
				return 1;
			}
			else
				return 0;
		}

		cur_page_w->GetFirst(&fetchme);
	}
	else
		return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{

	Record rec;
	//cout<<"In gn\n";
	while(this->GetNext(rec))
	{
		//cout<<"INW\n";	
		if (comp_eng.Compare(&rec, &literal, &cnf)) 
		{
			//cout<<"returning 1\n";
			fetchme.Consume(&rec);
			return 1;
		}
	}
	return 0;
}
