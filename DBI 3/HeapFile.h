#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

// typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class HeapFile : virtual public GenericDBFile{
	File *cur_file;
	ComparisonEngine comp_eng; 
	Page *cur_page_r; 
	Page *cur_page_w;
	int page_no;
	char *file_name;
	int  IsReadBuffEmpty();
	

public:
	HeapFile (); 
	~HeapFile();
	File *getFile();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
