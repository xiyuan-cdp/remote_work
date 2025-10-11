import requests
from DataRecorder import Recorder

cookies = {
    'Hm_lvt_66c58514bc6f9bc81f2ca3f9d9fa7a75': '1760172427',
    'HMACCOUNT': 'A89E656BF367E71C',
    'Hm_lpvt_66c58514bc6f9bc81f2ca3f9d9fa7a75': '1760172597',
}

headers = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Referer': 'https://easynomad.cn/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
    'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}


def get_data(page):
    params = (
        ('limit', '100'),
        ('page', page),
        ('jobCategory', ''),
        ('contractType', ''),
    )

    response = requests.get('https://easynomad.cn/api/posts/list', headers=headers, params=params, cookies=cookies)
    data = response.json()['data']
    print(data)
    return data


def get_list():
    page = 1
    while True:
        data = get_data(page)
        page += 1
        if not data:
            break
        r.add_data(data)


if __name__ == '__main__':
    r = Recorder('data.xlsx')
    get_list()  # 获取数据并写入excel
    r.record()  # 记录数据
