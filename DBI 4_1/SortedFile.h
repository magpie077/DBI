#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
#include "GenericDBFile.h"
#include "Pipe.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "DBFile.h"

using namespace std;

typedef struct {
        Pipe *pipe_ptr;
        OrderMaker *ord_ptr;
        Schema *sch_ptr;
        File *f_ptr;
        char* ldpath_ptr;


}util;

typedef enum {R, W} mode;


class SortedFile :virtual public  GenericDBFile {
        File f_ptr;
public:


    SortedFile ();
    mode status;
    int Create (char *fpath, fType file_type, void *startup);
    int Open (char *fpath);
    int Close ();
    void Load (Schema &myschema, char *loadpath);
	void SwitchMode(mode change);
    void FixDirtyFile();
    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    
    ~SortedFile();
};
#endif