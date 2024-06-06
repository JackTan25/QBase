extern "C"{
    #include "postgres.h"
    #include "executor/spi.h"
    #include "utils/rel.h"
    #include "access/htup_details.h"
    #include "utils/builtins.h"
}
#include<vector>
#include<string>
bool filter_tuple(ItemPointer tid,std::string relname_text,std::string filter_expr_text);