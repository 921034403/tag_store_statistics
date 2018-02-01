# _*_ coding:utf-8 _*_

from store import main as data_analysis_storecumulate
from tag import main as data_analysis_keywordcumulate
from mall_click import main as data_analysis_mallclickcumulate
from streammsg import main as data_analysis_upstreammsgday


if __name__ == '__main__':
    data_analysis_storecumulate()
    data_analysis_mallclickcumulate()
    data_analysis_upstreammsgday()
    data_analysis_keywordcumulate()