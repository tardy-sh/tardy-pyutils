import polars as pl


def unnest_dataframe(df: pl.DataFrame):
    while any(x == pl.Struct for x in df.dtypes):
        df = df.unnest(*[col_name for col_name, dtype in df.schema.items() if dtype == pl.Struct], separator=".")
    return df


def unnest_lazyframe(df):
    exprs = []

    def parse_struct_field(struct_field, base_expr, base_name):
        ret_list = []
        for field in struct_field.fields:
            if field.dtype == pl.Struct:
                ret_list.extend(
                    parse_struct_field(field.dtype, base_expr.struct.field(field.name), f"{base_name}.{field.name}")
                )
            else:
                ret_list.append(base_expr.struct.field(field.name).alias(f"{base_name}.{field.name}"))
        return ret_list

    # first_level_structs = [x for x, y in df.schema.items() if y == pl.Struct]
    sch = df.collect_schema()
    cols = sch.names()
    for col in cols:
        if sch[col] == pl.Struct:
            exprs.extend(parse_struct_field(sch[col], pl.col(col), col))
        else:
            exprs.append(col)
    return df.select(*exprs)
