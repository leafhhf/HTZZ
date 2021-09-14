
list=[['', None, None, '', None, None, '', None, None, '', None], ['', '', '', '', '2018年', '', '', '2017年', '', '', '变动比例'], ['', None, None, '', None, None, '', None, None, '', None], ['', None, None, '', None, None, '', None, None, '', None], ['', '研发人员数量（人）', '', '', '35', '', '', '22', '', '', '59.09%'], ['', None, None, '', None, None, '', None, None, '', None], ['', None, None, '', None, None, '', None, None, '', None], ['', '研发人员数量占比', '', '', '0.94%', '', '', '2.95%', '', '', '-2.01%'], ['', None, None, '', None, None, '', None, None, '', None], ['', None, None, '', None, None, '', None, None, '', None], ['', '研发投入金额（元）', '', '', '6,527,348.59', '', '', '4,686,715.25', '', '', '39.27%'], ['', None, None, '', None, None, '', None, None, '', None], ['', None, None, '', None, None, '', None, None, '', None], ['', '研发投入占营业收入比例', '', '', '0.26%', '', '', '0.11%', '', '', '0.15%'], ['', None, None, '', None, None, '', None, None, '', None], ['', None, None, '', None, None, '', None, None, '', None], ['', '研发投入资本化的金额（元）', '', '', '-', '', '', '-', '', '', '-'], ['', None, None, '', None, None, '', None, None, '', None], ['', None, None, '', None, None, '', None, None, '', None], ['', '资本化研发投入占研发投入的比例', '', '', '-', '', '', '-', '', '', '-'], ['', None, None, '', None, None, '', None, None, '', None]]

def select_and_delete(list):
    new_list = []
    for i in range(len(list)):
        new_list_temp = []
        for j in range(len(list[i])):           
            if list[i][j] is None:
                continue
            else:
                new_list_temp.append(list[i][j])
                #print(new_list_temp,'sss')
       # print(len(new_list[i]),'aaa')
       # if len(new_list[i])==0:
       #     continue
        if new_list_temp!=['', '', '', '']:
            print('new',new_list_temp)
            new_list.append(new_list_temp)

    return new_list
deleteafter = select_and_delete(list)
print(deleteafter)
