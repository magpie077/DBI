#include "gtest/gtest.h"
#include "DBFile.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.cc"
#include <iostream>
using namespace std;

TEST(OpenFnTest, t1)
{
	char *file_path = "gtest/openfiletest";
	fType type = heap;
    DBFile file;
    EXPECT_EQ(1, file.Create(file_path, type, NULL));
    file.Close();
}

TEST(CloseFnTest, t2) 
{
    DBFile file;
    int return_value = file.Close();
    EXPECT_EQ(1, return_value);
}

TEST(CreateFnTest, t3) 
{
    char *file_path = "gtest/openfiletest";
    DBFile file;
    int return_value = file.Open(file_path);
    EXPECT_EQ(1, return_value);
    file.Close();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}