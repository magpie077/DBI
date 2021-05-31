#include "gtest/gtest.h"
extern "C" {
	#include "y.tab.h"
}
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;


char *fileName = "Statistics.txt";

TEST (Correctness_test1, t1) {
	
	
	Statistics s;
        char *relName[] = {"lineitem"};

	s.AddRel(relName[0], 6001215);
	s.AddAtt(relName[0], "l_returnflag",3);
	s.AddAtt(relName[0], "l_discount",11);
	s.AddAtt(relName[0], "l_shipmode",7);
	
	char *cnf = "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";

	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 1);
	
	ASSERT_NEAR (8.5732e+5, result, 5.0);

	s.Apply(final, relName, 1);

	s.Write(fileName);
	
}



TEST (Correctness_test2, t2) {
	
	
	Statistics s;
    char *relName[] = {"orders","customer","nation"};

	
	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_custkey",150000);

	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	char *cnf = "(c_custkey = o_custkey)";
	yy_scan_string(cnf);
	yyparse();

	s.Apply(final, relName, 2);
	
	cnf = " (c_nationkey = n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName, 3);
	
	ASSERT_NEAR (1500000, result, 0.1);
	
	s.Apply(final, relName, 3);

	s.Write(fileName);	
	
}


int main(int argc, char *argv[]) {
	
	testing::InitGoogleTest(&argc, argv); 
	
	return RUN_ALL_TESTS ();
	
}