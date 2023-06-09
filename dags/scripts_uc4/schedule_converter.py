import xml.etree.cElementTree as et

# /////FYI/////
# # DAY CONFIGURATION FROM XML
# mondaxml = "EV_MONDAY"
# tuesday_xml = "EV_THUESDAY"
# wednesday_xml = "EV_WEDNESDAY"
# thursday_xml = "EV_THURSDAY"
# friday_xml = "EV_FRIDAY"
# saturday_xml = "EV_SATURDAY"
# sunday_xml = "EV_SUNDAY"

# # DAY CONFIGURATION IN RRULE
# monday_rrule = "MO"
# tuesday_rrule = "TU"
# wednesday_rrule = "WE"
# thursday_rrule = "TH"
# friday_rrule = "FR"
# saturday_rrule = "SA"
# sunday_rrule = "SU"

def ScheduleConverter(schedule_file_name, plan_name):
    tree=et.parse('dags/schedules/' + schedule_file_name + '.xml')
    root=tree.getroot()

    for task in root.iter('task'):

        # FIND ALL THE NECESSARY TAGS + SLICE HOUR AND MINUTES
        if task.get('Object') == plan_name:
            name_list = task.get('Object')
            after_tag = task.find('after')
            erlst_st_time = after_tag.get('ErlstStTime')
            hour = erlst_st_time[0:2]
            minutes = erlst_st_time[3:5]
            
            # WHEN JOB IS CONTINUE
            if ".C." in name_list:
                for task in root.iter('task'):
                    if task.get('Object') == plan_name:
                        cron = minutes + " " + hour + " * * *"
                        return cron

            # WHEN JOB IS EXECUTED DAILY
            if ".D." in name_list:
                for task in root.iter('task'):
                    if task.get('Object') == plan_name:
                        cron = minutes + " " + hour + " * * *"
                        return cron

            #WHEN JOB IS EXECUTED WEEKLY        
            if ".W." in name_list:
                for task in root.iter('task'):
                    if task.get('Object') == plan_name:
                        cale_tag = task.find('calendars/cale')
                        cale_key_name = cale_tag.get('CaleKeyName')
                        if cale_key_name[3:] == "MONDAY":
                            cron = minutes + " " + hour + " * * 1"
                        if cale_key_name[3:] == "THUESDAY":
                            cron = minutes + " " + hour + " * * 2"
                        if cale_key_name[3:] == "WEDNESDAY":
                            cron = minutes + " " + hour + " * * 3"
                        if cale_key_name[3:] == "THURSDAY":
                            cron = minutes + " " + hour + " * * 4"
                        if cale_key_name[3:] == "FRIDAY":
                            cron = minutes + " " + hour + " * * 5"
                        if cale_key_name[3:] == "SATURDAY":
                            cron = minutes + " " + hour + " * * 6"
                        if cale_key_name[3:] == "SUNDAY":
                            cron = minutes + " " + hour + " * * 0"
                        return cron

            # WHEN JOB IS EXECUTED MONTHLY        
            if ".M." in name_list:
                for task in root.iter('task'):
                    if task.get('Object') == plan_name:
                        cale_tag = task.find('calendars/cale')
                        cale_key_name = cale_tag.get('CaleKeyName')
                        date = cale_key_name[3:5].replace("T", "")
                        cron = minutes + " " + hour + " " + date + " * *"
                        return cron