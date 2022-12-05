#include <cs165_api.h>
#include <Serializer/serialize.h>

size_t writing_space(Column *column)
{
    return column->file_size - (column->end + sizeof(ColumnMetaData));
}