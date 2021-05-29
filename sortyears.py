'''Move files into folders by years.

Author: guangzhi XU (xugzhi1987@gmail.com)
Update time: 2021-05-29 20:05:08.
'''

from __future__ import print_function
import os
import shutil
import glob
from datetime import datetime


#--------------Globals------------------------------------------
PREFIX='EI'
DIR='/home/guangzhi/datasets/flexpart_erai/outputs'               # folder to save outputs
DIR='/run/media/guangzhi/Elements SE/outputs'
OUTPUTDIR='/home/guangzhi/datasets/flexpart_erai/outputs'               # folder to save outputs
OUTPUTDIR='/run/media/guangzhi/Elements SE/outputs'










#-------------Main---------------------------------
if __name__=='__main__':

    files = os.listdir(DIR)
    years = []
    for fii in files:
        dateii = fii.strip(PREFIX)
        yearii = dateii[:2]
        years.append(yearii)

    years = list(set(years))
    years.sort()

    __import__('pdb').set_trace()
    for yearii in years:
        dtii = datetime.strptime(str(yearii), '%y')
        year4ii = dtii.strftime('%Y')
        dirii = os.path.join(OUTPUTDIR, year4ii)
        print('Sorting year', yearii, year4ii, 'folder:', dirii)

        if not os.path.exists(dirii):
            os.makedirs(dirii)

        globstr = os.path.join(DIR, '%s%s*' %(PREFIX, yearii))
        filesii = glob.glob(globstr)

        for fii in filesii:
            fnameii = os.path.split(fii)[1]
            tii = os.path.join(dirii, fnameii)
            print('moving file', fii, '->', tii)
            shutil.move(fii, tii)





