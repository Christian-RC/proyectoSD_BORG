import socket
import datetime
import time
import sys
import json
import operator
from concurrent.futures import ThreadPoolExecutor
import logging
import sys

logging.basicConfig(level=logging.DEBUG, format='%(threadName)s: %(message)s')

term = ""  # mensaje que sera enviado al nodo maestro
tareas = []
cuota = 1000
cuota_aux = []
cuota_aux.append(cuota)

def desplazamientos():
	while True:
	    s = socket.socket()  # creacion de scoket

	    host = 'localhost'  # configuracion para localhost y puerto 8000
	    port = 8000
	    s.connect((host, port))  # creando conexion

	    term = str(cuota_aux)
	    s.sendall(str(term).encode())
	    tserver = s.recv(1024).decode()

	    tserver = json.loads(tserver)

	    name_tasks = []
	    priority_cuota_tasks = []
	    priority_tasks = []
	    cuota_tasks = []
	    worker_tasks = []

	    for tarea in tareas:
	    	name_tasks.append(list(tarea.keys())[0])
	    	priority_cuota_tasks.append(list(tarea.values())[0])

	    for priority_cuota in priority_cuota_tasks:
	    	priority_tasks.append(priority_cuota[0])
	    	cuota_tasks.append(priority_cuota[1])
	    	worker_tasks.append(priority_cuota[2])
	    
	    if len(tareas) == 0:
	    	tareas.append(tserver)
	    	for key in tserver.keys():		
	    		cuota_aux[0] = cuota_aux[0] - tserver[key][1]
	    else:
	    	pos = 0
	    	for name in name_tasks:
	    		for key in tserver.keys():
	    			if len(tareas) == 1:
	    				if tserver[key][0] < priority_tasks[pos]:
	    					tareas.insert(pos+1,tserver)
	    				else:
	    					tareas.insert(pos,tserver)
	    			elif (pos+1) < len(tareas) and tserver[key][0] > priority_tasks[pos]:
	    				tareas.insert(pos,tserver)
	    			elif (pos+1) < len(tareas) and tserver[key][0] < priority_tasks[pos] and tserver[key][0] > priority_tasks[pos+1]:
	    				tareas.insert(pos+1,tserver)
	    			else:
	    				tareas.append(tserver)
	    			cuota_aux[0] = cuota_aux[0] - tserver[key][1]
	    			find = True 
	    			break
	    		if find: 
	    			break 
	    		pos = pos + 1

	    logging.info('*DESPLAZAMIENTOS POR PRIORDIDADES* - Recursos restantes: ' + str(cuota_aux[0]))
	    print(str(tareas) + '\n')
	    time.sleep(2)


def ejecucion():
	while len(tareas) != 0:
		time.sleep(5)
		task = tareas.pop(0)
		for key in task.keys():
			cuota_aux[0] = cuota_aux[0] + task[key][1]
			logging.info('*EJECUTANDO* ' + str(task) + ' - Recursos restantes: ' + str(cuota_aux[0]) + '\n')
			break

if __name__ == '__main__':

	print("******************************* BORGLET  - CUOTA (" + str(cuota) + ") *******************************\n")

	executor = ThreadPoolExecutor(max_workers=2)

	executor.submit(desplazamientos)
	time.sleep(1)
	executor.submit(ejecucion)
	

	exit()

