#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
#include "GenericDBFile.h"

struct metadata {
        int runlength;
        OrderMaker sorting_order;
        fType filetype;

};

struct strtup {
        OrderMaker *ord;
        int l;

};


class DBFile {
        File *cur_file;
public:

        GenericDBFile* genericVar;
        DBFile ();
        ~DBFile();
        File *getFile();
        int Create (char *fpath, fType file_type, void *startup);
        int Open (char *fpath);
        int Close ();
        strtup *mysrt;

        void Load (Schema &myschema, char *loadpath);

        void MoveFirst ();
        void Add (Record &addme);
        int GetNext (Record &fetchme);
        int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif