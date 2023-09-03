# coding=utf-8 ##以utf-8编码储存中文字符
import os
import json
import re
import time
import logging
import pandas as pd
import warnings

warnings.simplefilter('ignore')


def read_config(configPath):
    """"读取配置"""
    with open(configPath, 'r', encoding="UTF-8") as json_file:
        config = json.load(json_file)

        # 文件路径
        dataPath = config["dataPath"]
        cityMapFile = config["cityMapFile"]
        # 列名
        cityName = config["cityName"]
        cityCode = config["cityCode"]
        # 文件输出最大行数
        maxLines = config["maxLines"]
        cityMap = pd.read_excel(cityMapFile)
        skipRows = config['skipRows']
        engine = config['engine']
        excelCityName = config["excelCityName"]
        # city_map = dict(zip(cityMap['城市'],cityMap['地市编码']))
        city_map = dict(zip(cityMap[cityName], cityMap[cityCode]))

    return config, dataPath, city_map, cityName, cityCode, maxLines, skipRows, engine, excelCityName


def getForm(config, skipRows, engine):
    # global form
    # skipRows = config['skipRows']
    # engine = config['engine']

    cityName = config["cityName"]
    # excelCityName = config["excelCityName"]
    rawForm = pd.DataFrame()
    suffix = formListPath.split(".")[-1]

    # if suffix == "xls":
    #     rawForm = pd.read_excel(formListPath, skiprows=skipRows, engine="xlrd")  # 去掉第一行
    # elif suffix == "xlsx":
    #     rawForm = pd.read_excel(formListPath, skiprows=skipRows, engine="openpyxl")  # 去掉第一行

    rawForm = pd.read_excel(formListPath, skiprows=skipRows, engine=engine)  # 去掉第一行
    rawForm = rawForm.rename(columns={excelCityName: cityName})

    regex = r'^' + cityName + '$|' + '\d{4}[A-Za-z].*|^\d{4}$'
    match_result = rawForm.columns.map(str).str.match(regex)
    needColumns = rawForm.columns[match_result]
    form = rawForm[needColumns]
    # print(form.head(5))

    filteredColumns = rawForm.columns[~match_result]
    filteredForm = rawForm[filteredColumns]

    return form, filteredForm


def saveAns():
    global saveName, savePath, f, s
    saveName = "11_" + str(
        int(float(key))) + "_Incremental_" + timeNow + "_" + str(count).zfill(4) + ".dat"
    savePath = "./save/" + dirList + '/' + saveName

    f = open(savePath, 'w', encoding="UTF-8")  # 创建文件
    f.close()
    with open(savePath, 'a', newline='\n', encoding="UTF-8") as f:
        # f.write(bytes(ans + '\n',encoding = 'utf-8'))
        # f.write(ans + '\n')
        s = ""
        for x in ans:
            s += str(x)
        f.write(s)


def deal():
    global ans, timeNow, key, count, code4
    # 2、处理
    # 行内遍历

    ans = []
    timeNow = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))

    for index, row in form.iterrows():  # 遍历行 row是一个series

        # 由城市找到 code4
        city = row[cityName]

        if city in city_map:
            code4 = city_map[city]

        # 非城市的列
        cols = [i for i in form.columns if i not in [cityName]]
        if str(city) == 'nan' or cols == []:
            logger.info(formListPath + "-----> 第" + str(
                index + 1 + skipRows + 1) + "行数据,未处理：是非目标行数据或非目标列数据")
            continue

        # 行内遍历
        for key, value in row[cols].items():  # item 是每个单元格内的字符串
            key = str(key)[:4]  # 号段
            # 对非空的数值进行处理
            if not pd.isna(value):
                # 拼接
                nums = re.split(r',|、|，', str(value))
                code3Temp = []
                for num in nums:
                    if '-' in num:  # 区间数据
                        start, end = num.split('-')
                        # start = start.rstrip('0').rstrip('.')  # 删除小数点后多余的0和小数点
                        # end = end.rstrip('0').rstrip('.')  # 删除小数点后多余的0和小数点
                        if len(start) != 3 or len(end) != 3:
                            logger.info(formListPath + "-----> 第" + str(
                                index + 1 + skipRows + 1) + "行数据-----> 城市：" + city + ' ; ' + '号段：' + key + " 下的" + "单元格数据" + num + "：不符合要求,未处理")
                            continue
                        start = int(start)
                        end = int(end)
                        code3Temp.extend([str(i).zfill(3) for i in range(start, end + 1)])
                    else:  # 单个数据
                        # 是带小数点的浮点数
                        if '.' in str(num):
                            num_new = num.rstrip('0').rstrip('.')  # 删除小数点后多余的0和小数点
                        else:
                            num_new = num

                        if 0 < len(str(num_new)) <= 3:
                            code3Temp.append(str(int(float(num))).zfill(3))
                        if len(str(num_new)) > 3:
                            logger.info(formListPath + "-----> 第" + str(
                                index + 1 + skipRows + 1) + "行数据-----> 城市：" + city + '; ' + '号段：' + key + "; 下的" + "单元格数据" + num + "：不符合要求,未处理")
                            continue

                # 3、保存结果
                for i in code3Temp:
                    count = 0
                    if (len(ans) >= maxLines):
                        saveAns()
                        ans = []
                        count += 1

                    code3 = key + i
                    selectAD = config["selectAD"]
                    # timeNow 读入该表格的时间
                    ans.append(selectAD + '|' + timeNow + '|' + code3 + '|' + str(code4) + '\n')
    if ans != []:
        saveAns()


def mode_select():
    global skipRows, engine, excelCityName
    if config["Auto"] == "Yes":

        if "移动" in formListPath:
            skipRows = 1
            engine = "openpyxl"
            excelCityName = '城市'

        if "联通" in formListPath:
            skipRows = 0
            engine = "openpyxl"
            excelCityName = '所辖城市'

        if "广电" in formListPath:
            skipRows = 1
            engine = "openpyxl"
            excelCityName = '城市'

        if "电信" in formListPath:
            skipRows = 2
            engine = "xlrd"
            excelCityName = '城市'


def logConfig():
    global logger
    # 第一步：创建日志器对象，默认等级为warning
    logger = logging.getLogger("日志")
    logging.basicConfig(level="INFO")
    # 第二步：创建控制台日志处理器+文件日志处理器
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(config["logSave"], mode="a", encoding="utf-8")
    # 第三步：设置控制台日志的输出级别,需要日志器也设置日志级别为info；----根据两个地方的等级进行对比，取日志器的级别
    console_handler.setLevel(level="WARNING")
    # 第四步：设置控制台日志和文件日志的输出格式
    console_fmt = "%(lineno)--->d%(levelname)s--->%(asctime)s--->%(message)s"
    file_fmt = "%(lineno)d--->%(levelname)s--->%(asctime)s--->%(message)s"
    fmt1 = logging.Formatter(fmt=console_fmt)
    fmt2 = logging.Formatter(fmt=file_fmt)
    console_handler.setFormatter(fmt=fmt1)
    file_handler.setFormatter(fmt=fmt2)
    # 第五步：将控制台日志器、文件日志器，添加进日志器对象中
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


if __name__ == '__main__':

    # 0、读取配置
    configPath = "./config/config.json"
    config, dataPath, city_map, cityName, cityCode, maxLines, skipRows, engine, excelCityName = read_config(configPath)
    # 日志设置
    logConfig()

    # 1、读取文件
    dirLists = os.listdir(dataPath)  # 一级目录：文件夹名字 列表

    for dirList in dirLists:

        excelLists = os.listdir(dataPath + dirList)  # 二级目录：表格名字 列表
        # print(excelPaths)
        for excelPath in excelLists:

            formListPath = dataPath + dirList + '/' + excelPath  # 表格全路径
            # 使用内置参数
            mode_select()
            # 设置输出路径
            if not os.path.exists(config["batSave"] + dirList):
                os.makedirs(config["batSave"] + dirList)

            try:
                # 获取需要处理的表格form
                form, filteredForm = getForm(config, skipRows, engine)
                # 2、进行表格处理 和 保存文件
                deal()
            except Exception as result:
                # 打印错误信息
                logger.error(formListPath + '-----> : ' + str(result))
                # print(formListPath + ': ' + str(result))

        print(dirList + "is over!")

    print('Process over!')
    # 休眠5秒
    time.sleep(5)
