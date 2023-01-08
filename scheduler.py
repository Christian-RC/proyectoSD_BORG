import socket
import datetime
import json

print("****************************** SCHEDULER ******************************\n")

s = socket.socket()  # creacion de scoket
print("Socket creado correctamente")

host = ''   # configuracion para todas las interfaces disponibles y con puerto 8000
port = 8000
s.bind((host, port))
s.listen(5)  # socket escuchando
print("Iniciando proceso de sincronizacion...")

#{name_task: [priority_task, cuota_task, worker]}
cola_tareas = [{"task_0":[10, 100, 'worker_1']}, {"task_1":[5, 150, 'worker_3']}, {"task_2":[7, 200, 'worker_2']}, {"task_3":[9, 80, 'worker_1']}, {"task_4":[3, 100, 'worker_2']}, {"task_5":[11, 200, 'worker_3']}]

while True:
    dict = {}

    connection, address = s.accept()
    data = connection.recv(1024).decode()
    if not data:
        break

    print('Cola de tareas:')
    for tarea in cola_tareas:
        print(tarea)
    
    if len(cola_tareas) == 0:
        print("La cola de tareas se encuentra vacia\n")
        exit()
    else:
        connection.send(json.dumps(cola_tareas.pop(0)).encode())
        print("\nASIGNANDO TAREA - cuota restante antes de asignacion: " + data + "\n")
        connection.close()