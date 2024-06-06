#include "spi.h"

bool filter_tuple(ItemPointer tid,std::string relname_text,std::string filter_expr_text)
{
    char ctid_str[32];
    snprintf(ctid_str, sizeof(ctid_str), "(%u,%u)", ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));

    char *query = psprintf("SELECT 1 FROM %s WHERE ctid = '%s' AND %s", relname_text.c_str(), ctid_str, filter_expr_text.c_str());

    int ret;
    bool result = false;

    SPI_connect();
    
    ret = SPI_exec(query, 1);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        // we can get data, it means we don't filter data
        result = true;
    }

    SPI_finish();

    return result;
}
