#include "util.h"
#include <vector>
// #include <cstring>
#include <string>
extern "C"
{
#include <postgres.h>
}

void Print()
{
    std::string s = "anc";
    std::vector<int> v;
    v.push_back(1);
    v.push_back(2);
    for (int i = 0; i < v.size(); i++)
    {
        elog(INFO, "vec %d", v[i]);
    }
    elog(INFO, "string %s:", s);
    elog(INFO, "Print");
}