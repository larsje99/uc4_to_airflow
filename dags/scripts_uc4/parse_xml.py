import xml.etree.cElementTree as et

class ParseXML:
    def PlaceJobsInList(plan_file, plan):
        try:
            # Parse the XML file
            tree = et.parse('dags/jobs/' + plan_file + '.xml')
            root = tree.getroot()

            # Find the JOBP tag with the specified value
            jobp_tag = root.find(f".//JOBP[@name='{plan}']")

            # Find the Object values within the task tags inside the JOBP tag
            task_objects = []
            for task in jobp_tag.findall('.//task'):
                object_value = task.get('Object')
                task_objects.append(object_value)

            return task_objects
        except:
            return "PLAN DEPENDENCIES ARE NOT SUPPORTED YET"

    def GetQueueName(plan_file):
        tree=et.parse('dags/jobs/' + plan_file + '.xml')
        root=tree.getroot()

        for queue in root.iter('Queue'):
            queue_list = queue.text

        return queue_list

    def GetBashCommand(plan_file, job):
        try:
            if job == "START":
                bash_command = 'echo "FLOW SUCCESFULLY STARTED"'
                return bash_command
            if job == "END":
                bash_command = 'echo"FLOW SUCCESFULLY ENDED"'
                return bash_command
            else:
                # Parse the XML file
                tree = et.parse('dags/jobs/' + plan_file + '.xml')
                root = tree.getroot()

                # Find the JOBP tag with the specified value
                jobp_tag = root.find(f".//JOBS_UNIX[@name='{job}']")

                for script in jobp_tag.findall('.//MSCRI'):
                    bash_command = script.text

                return bash_command
        except:
            bash_command = 'echo "PLAN DEPENDENCIES ARE NOT SUPPORTED YET"'
            return bash_command
        
    def GetDependencies(plan_file, job):
        try:
            dependencies = []
            
            # Parse the XML file
            tree = et.parse('dags/jobs/' + plan_file + '.xml')
            root = tree.getroot()

            # Find the JOBP tag with the specified value
            task_tag = root.find(f".//task[@Object='{job}']")

            for task in task_tag.findall('.//pre'):
                prelnr_value = task.get('PreLnr')
                dependencies.append(prelnr_value)  # Append dependency value to the list

            return dependencies  # Return the list of dependency values
        except:
            return ["1"]  # Return a list containing "1" if an exception occurs
