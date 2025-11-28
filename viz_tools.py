"""
Модуль с функциями визуализации и расчета для
показа мета данных PARQUET файла
"""

from IPython.display import HTML, Javascript
import ipywidgets as widgets

import duckdb

def showOverview(pqFile):
    """ показывает общую информацию о файле (в виде текста плюс .show() от duckdb """
    global PQ_FILE, meta, grpNum

    PQ_FILE = pqFile

    meta = duckdb.sql(f"SELECT * FROM parquet_metadata('{PQ_FILE}')")
    sch = duckdb.sql(f"SELECT * FROM parquet_schema('{PQ_FILE}')")
    grpNum = duckdb.sql("select max(row_group_id)+1 from meta").df().values.tolist()[0][0]

    print("FILE:",PQ_FILE)
    print("row number       :",int(duckdb.sql("select sum(num_values) from meta where column_id=0").df().values.tolist()[0][0]))
    print("row groups number:",grpNum)
    print("row_group sizes  :")
    duckdb.sql("select num_values as 'rows in row_group', count(*) as 'number of row_groups' from meta where column_id=0 group by 1").show()

def findColumn(offset):
    """ показывает, в какую row_group и какую колонку попадает заданное смещение """
    global PQ_FILE, meta, grpNum

    # проверим - вдруг попали на точное начало данных колонки в пределах группы
    res = duckdb.sql(f"select row_group_id,path_in_schema from meta where data_page_offset={offset}").df()
    if len(res)>0:
        print("Row_group:",res.values.tolist()[0][0])
        print("Column   :",res.values.tolist()[0][1])
    else: # было указано какое-то смещение... ищем
        prevOffset = 0
        for row in duckdb.sql("select row_group_id,min(data_page_offset) from meta group by 1 order by 1").df().values.tolist():
            if offset>=prevOffset and offset<=row[1]: # текущая row группа начинается с бОльшего смещения
                rowGroup = row[0]-1
                prevOffset = 0
                prevColumn = ""
                for cols in duckdb.sql(f"select path_in_schema,data_page_offset from meta where row_group_id={rowGroup} order by 2").df().values.tolist():
                    if offset>=prevOffset and offset<=cols[1]: # предыдущая колонка - наша
                        print("Offset belongs to")
                        print("Row_group:",rowGroup)
                        print("Column   :",prevColumn)
                        print("Starts at:",prevOffset)
                        return          
                    prevOffset = cols[1]
                    prevColumn = cols[0]
                print("Offset does not belong to any column within row group...") # если на такое реально напоремся - буду разбираться...
            prevOffset = row[1]    
        print("Offset does not belong to any row group...") # если на такое реально напоремся - буду разбираться...

def printColInfo(rowGroupInd,colName):
    """ печатает информацию о колонке row group по ее имени """
    
    resDf = duckdb.sql(f"""
        select
            column_id,
            type, 
            stats_min_value, 
            stats_max_value, 
            encodings, 
            index_page_offset, 
            bloom_filter_offset,
            bloom_filter_length,
            compression,
            data_page_offset,
            dictionary_page_offset,
            total_compressed_size,
            num_values,
            stats_null_count,
            stats_distinct_count
        from meta 
        where 
            row_group_id={rowGroupInd} 
            and path_in_schema='{colName}'
    """).df()
    resRow = dict(zip(resDf.columns,resDf.iloc[0]))
    print("Column :",colName, "(", resRow["type"], ") #", resRow["column_id"]) 
    print("count/null/distinct:",f'{resRow["num_values"]}/{resRow["stats_null_count"]}/{resRow["stats_distinct_count"]}') 
    print("stats_min_value    :",resRow["stats_min_value"]) 
    print("stats_max_value    :",resRow["stats_max_value"]) 
    print("encodings          :",resRow["encodings"]) 
    print("index_page_offset  :",resRow["index_page_offset"]) 
    print("bloom_filter_offset:",resRow["bloom_filter_offset"])
    print("bloom_filter_length:",resRow["bloom_filter_length"])
    print("compression        :",resRow["compression"])
    print("data_page_offset   :",resRow["data_page_offset"])
    print("dict_page_offset   :",resRow["dictionary_page_offset"])
    print("compressed_size    :",resRow["total_compressed_size"])

def getExcludedGroupsStr(colName,colValue):
    """ возвращает строку с перечнем исключенных row group """

    global PQ_FILE, grpNum

    res = duckdb.sql(f"select row_group_id from parquet_bloom_probe('{PQ_FILE}', '{colName}', {colValue}) where bloom_filter_excludes").df().values
    if len(res)>0:
        grList = res.tolist()
        if len(grList)<grpNum:
            return f"""Following row groups will be excluded: {",".join([str(r[0]) for r in grList])}"""
        else:
            return "All row groups will be excluded"
    else:
        return "No row groups will be excluded"

def doPrepLists(cols,groups,showData=False,bloomCols=None):

    valText = widgets.Text(
        value='',
        description='Value:',
        layout = widgets.Layout(width='200px'),
        disabled=False   
    )
    colList = widgets.Dropdown(
        options=bloomCols if bloomCols else cols,
        layout = widgets.Layout(width='200px'),
        description='Column:'
    )
    grpList = widgets.Dropdown(
        options=groups,
        layout = widgets.Layout(width='200px'),
        description='Row group:'
    )

    showBtn = widgets.Button(
        description='Show info',
        disabled=False,
        button_style='info', 
        tooltip='Prepare list for playing in player',
    )

    def doShow(b):
        
        outList.clear_output()
        with outList:
            if bloomCols:
                print("(working...)")
                res = getExcludedGroupsStr(colList.value, valText.value)
                outList.clear_output()
                print(res)
            else:
                printColInfo(grpList.value,colList.value)

    showBtn.on_click(doShow)

    outList = widgets.Output(layout={'border': '1px solid black'})
    if bloomCols:
        display(widgets.HBox([colList,valText,showBtn]))
    else:
        display(widgets.HBox([colList,grpList,showBtn]))
    display(outList)

    if showData:
        doShow(None)

def showColumDetals():
    """ готовит виджеты и показывает информацию о колонке №0 из группы №0 """

    # colRes = duckdb.sql(f"""select distinct(path_in_schema) from meta where row_group_id=0""").df().values.tolist()
    colRes = duckdb.sql(f"""select path_in_schema from meta where row_group_id=0""").df().values.tolist()
    colList = [ c[0] for c in colRes ]

    doPrepLists(colList, range(grpNum), True)

def probeBloomFilters():
    """ готовит виджеты для показа проверки bloom filters """

    colRes = duckdb.sql(f"""select distinct(path_in_schema) from meta where bloom_filter_offset is not NULL""").df().values.tolist()
    colList = [ c[0] for c in colRes ]

    doPrepLists(colList, range(grpNum), False, colList)
