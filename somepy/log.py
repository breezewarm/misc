import pandas as pd
import re

keyfile = '/Users/cwen/development/doc/RWJF_project/phase_2/log_for_josh/key_for_chen.csv'
infile = '/Users/cwen/development/doc/RWJF_project/phase_2/log_for_josh/allUIAccess.log'
outfile = '/Users/cwen/development/doc/RWJF_project/phase_2/log_for_josh/allUIAccess.processed.log'
outcheckfile = '/Users/cwen/development/doc/RWJF_project/phase_2/log_for_josh/checkMapping.log'
nosidfile = '/Users/cwen/development/doc/RWJF_project/phase_2/log_for_josh/nosid.log'

dtype_dict = {'PAT_ID': str, 
            'PAT_MRN_ID': str,
            'sid': str}
keys = pd.read_csv(keyfile, header=0, dtype=dtype_dict)
keys.columns = ['pid', 'mrn', 'sid']
#keys
#print(keys.loc[keys['mrn'] == '']['sid'])
# mrn_key = keys[keys['mrn'] == ''] # return pandas.core.series.Series
# print(mrn_key['sid'].values[0])

with open(outfile, 'w', buffering=1) as out_f:
    with open(infile) as f:
        with open(outcheckfile, 'w', buffering=1) as check_f:
            with open(nosidfile, 'w', buffering=1) as nosid_f:
                line = f.readline()
                while line:
                    if 'mrn=' in line:
                        [head, mrn_log] = re.search(r'(mrn=)([0-9]*)(.*?)', line).group(0).split('=')
                        mrn = str(mrn_log).zfill(9)
                        try:
                            sid = keys[keys['mrn'] == str(mrn)]['sid'].values[0]
                            replaced_log = re.sub(r'(mrn=)([0-9]*)(.*?)', 'study_id=' + sid, line)
                            out_f.write(replaced_log)
                            check_f.write('%s : %s \n' % (mrn_log, sid))
                        except IndexError:
                            out_f.write(re.sub(r'(mrn=)([0-9]*)(.*?)', 'mrn=*********', line))
                            nosid_f.write('%s \n' % (mrn_log))
                    if '.out.html' in line:
                        out_f.write(line)
                    line = f.readline()