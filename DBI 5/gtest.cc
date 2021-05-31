#include "gtest/gtest.h"
#include "DBMgmt.h"

extern "C" {
    int yyparse(void);
    struct YY_BUFFER_STATE *yy_scan_string(const char *);
};

TEST(DBMgmt, ExecuteQueryStatusTest) {
    DBMgmt dbMgmt;

    // status should be 0 for SET operation.
    char *query = "SET OUTPUT STDOUT;";
    yy_scan_string(query);

    int status = dbMgmt.ExecuteQuery();
    EXPECT_EQ(status, 0);

    query = "STOP;";
    yy_scan_string(query);

    status = dbMgmt.ExecuteQuery();
    EXPECT_EQ(status, 1);
}

TEST(DBMgmt, OutputTypeTest) {
    DBMgmt dbMgmt;

    // Default output should be STD_TYPE
    ASSERT_EQ(dbMgmt.queryOutputType, STD_TYPE);

    char *query = "SET OUTPUT 'someFile';";
    yy_scan_string(query);
    dbMgmt.ExecuteQuery();
    ASSERT_EQ(dbMgmt.queryOutputType, FILE_TYPE);

    query = "SET OUTPUT NONE;";
    yy_scan_string(query);
    dbMgmt.ExecuteQuery();
    ASSERT_EQ(dbMgmt.queryOutputType, NONE_TYPE);

    query = "SET OUTPUT STDOUT;";
    yy_scan_string(query);
    dbMgmt.ExecuteQuery();
    ASSERT_EQ(dbMgmt.queryOutputType, STD_TYPE);
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}