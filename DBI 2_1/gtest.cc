#include <bits/stdc++.h> 
#include "gtest/gtest.h"
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "TwoWayList.cc"
#include <iostream>
#include <pthread.h>
#include "BigQ.h"
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Pipe.h"
#include "test.h"


TEST(OpenBinTest, t1) 
{
    int return_value;
    DBFile db;
    return_value = db.Open ("bigqTemp.bin");
    EXPECT_TRUE(return_value == 1);
    db.Close ();
}

TEST( BinFileNotEmptyTest, t2)
{
    int isNotEmpty;
    Record temp;
    int counter = 0;

    DBFile dbfile;
    dbfile.Open ("nation.bin");
    dbfile.MoveFirst ();
    isNotEmpty = dbfile.GetNext (temp);
    EXPECT_TRUE(isNotEmpty == 1);
}

TEST(InsertToPipeTest, t3)
{
    Pipe input (100);
    int hasRecords;
    Record temp, getRecord;
    int recordCounter = 0;
    DBFile db;
    db.Open ("nation.bin");
    db.MoveFirst ();
    while (db.GetNext (temp) == 1) {
        recordCounter += 1;
        if (recordCounter%100000 == 0) {
             cerr << " producer: " << recordCounter << endl;    
        }
        input.Insert(&temp);
    }

    hasRecords = input.Remove(&getRecord);
    db.Close ();
    input.ShutDown ();
    EXPECT_TRUE(hasRecords == 1);
}

TEST(WriteToFileTest, t4) 
{
    char *file_path = "gtest/gtesttempfile";
    fType file_type = heap;
    DBFile db;
    int a = db.Create(file_path, file_type, NULL);
    Schema mySchema ("catalog", "orders");
    Record record;
    FILE *orderFile = fopen ("orders.tbl", "r");
    record.SuckNextRecord(&mySchema,orderFile);
    db.Add(record);
    db.Close();
    int file  = db.Open(file_path);
    EXPECT_TRUE(db.getFile()->GetLength() > 0);
    db.Close();
}



int main(int argc, char **argv) 
{
    // DBFile db;
  ::testing::InitGoogleTest(&argc, argv); 
  return RUN_ALL_TESTS();
}