def parse_db_return(list):

    new_list = []

    for el in list:
        
        new_list.append(el[0])

    if len(new_list) > 1:

        return new_list
    
    else:

        return new_list[0]
