from azure.iot.device import IoTHubDeviceClient, Message
import time
import random
import json
import os
from datetime import datetime, timedelta

# --- Configuración ajustable - ¡MODIFICA ESTOS VALORES SEGÚN TUS NECESIDADES! ---
num_vehicles = 20  # Número de vehículos a simular
days_to_simulate = 7  # Simular una semana de datos
real_time_duration = 3600  # Duración en segundos para generar los datos (1 hora)
simulated_interval_minutes = 180  # Intervalo de tiempo simulado por mensaje (cada cuántos minutos simulados se envía un mensaje)

# --- Cálculo correcto de intervalos de tiempo para la compresión ---
total_simulated_minutes = days_to_simulate * 24 * 60  # Total de minutos a simular
num_messages_per_vehicle = total_simulated_minutes // simulated_interval_minutes  # Mensajes por vehículo

# Tiempo real que debe transcurrir entre cada envío de datos (para todos los vehículos)
delay_between_messages = real_time_duration / num_messages_per_vehicle

device_ids = [f"vehiculo_{i:03d}" for i in range(1, num_vehicles + 1)]
CONNECTION_STRING = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")

if not CONNECTION_STRING:
    print("ERROR: La variable de entorno 'IOTHUB_DEVICE_CONNECTION_STRING' no está configurada.")
    exit(1)

# Patrones de comportamiento por vehículo (para hacer los datos más realistas)
vehicle_profiles = {
    id: {
        'base_speed': random.uniform(30, 90),
        'base_temp': random.uniform(85, 95),
        'usage_pattern': random.choice(['urban', 'highway', 'mixed']),
        'odometer': random.uniform(5000, 150000)
    }
    for id in device_ids
}

def generate_vehicle_data(device_id, current_time):
    profile = vehicle_profiles[device_id]

    # Variaciones basadas en patrón de uso
    if profile['usage_pattern'] == 'urban':
        speed = round(random.uniform(10, 60), 2)
        rpm = random.randint(1000, 3000)
    elif profile['usage_pattern'] == 'highway':
        speed = round(random.uniform(80, 120), 2)
        rpm = random.randint(2500, 4500)
    else:  # mixed
        speed = round(random.uniform(20, 100), 2)
        rpm = random.randint(1500, 4000)

    # Variaciones diurnas/nocturnas
    hour = current_time.hour
    if 22 <= hour or hour < 6:  # Noche
        speed *= 0.7
        rpm *= 0.8

    # Generar datos base con variaciones realistas
    engine_temp = round(profile['base_temp'] + random.uniform(-2, 2), 2)
    vibration_level = round(random.uniform(0.1, 0.5), 2)
    fuel_level = round(random.uniform(10, 100), 2)

    # Generación de anomalías (5% probabilidad)
    if random.random() < 0.05:
        engine_temp = round(random.uniform(120, 150), 2)
        print(f"ANOMALÍA GENERADA: {device_id} - Temp: {engine_temp}°C en {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Incrementar odómetro basado en velocidad y el intervalo de tiempo simulado
    # Si la velocidad es en km/h, entonces (velocidad / 60) es km/minuto.
    # Multiplicamos por simulated_interval_minutes para obtener km en ese intervalo simulado.
    odometer_increment = (speed / 60) * simulated_interval_minutes
    profile['odometer'] += odometer_increment

    return {
        "deviceId": device_id,
        "timestamp": int(current_time.timestamp() * 1000),
        "latitude": round(random.uniform(-90, 90), 5),
        "longitude": round(random.uniform(-180, 180), 5),
        "speed_kmh": round(speed, 2),
        "engine_temp_c": engine_temp,
        "vibration_level": vibration_level,
        "fuel_level_percent": fuel_level,
        "door_open": random.random() < 0.1,  # 10% probabilidad
        "fuel_type": random.choice(["Gasoline", "Diesel", "Electric", "Hybrid"]),
        "battery_voltage": round(random.uniform(11.5, 14.8), 2),
        "oil_pressure_kpa": random.randint(150, 450),
        "engine_rpm": rpm,
        "odometer_km": round(profile['odometer'], 2)
    }

try:
    client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    client.connect()
    print(f"Generando datos para {num_vehicles} vehículos...")
    print(f"Simulando {days_to_simulate} días en {real_time_duration/3600:.2f} horas reales.")
    print(f"Cada mensaje representa {simulated_interval_minutes} minuto(s) simulado(s).")
    print(f"Intervalo entre envíos: {delay_between_messages:.4f} segundos reales.")
    print(f"Total de mensajes por vehículo: {num_messages_per_vehicle}")
    print(f"Total de mensajes en la simulación: {num_messages_per_vehicle * num_vehicles}")

    # La simulación comenzará desde la fecha/hora actual
    start_sim_time = datetime.utcnow()
    end_sim_time = start_sim_time + timedelta(days=days_to_simulate)
    current_sim_time = start_sim_time

    print(f"La simulación de datos comenzará en el tiempo simulado desde: {start_sim_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Y terminará en el tiempo simulado en: {end_sim_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("Iniciando envío de datos...")

    message_count = 0
    start_real_time = time.time()

    while current_sim_time < end_sim_time:
        # Enviar datos para todos los vehículos en este momento simulado
        for device_id in device_ids:
            data = generate_vehicle_data(device_id, current_sim_time)
            msg = Message(json.dumps(data))
            msg.custom_properties["deviceId"] = device_id

            try:
                client.send_message(msg)
                message_count += 1
                # Imprimir información cada cierto número de mensajes para no saturar la consola
                if message_count % (num_vehicles * 10) == 0:  # Cada 10 "rondas" de vehículos
                    print(f"Enviado: {current_sim_time.strftime('%Y-%m-%d %H:%M:%S')} - {device_id} - Vel: {data['speed_kmh']} km/h - Temp: {data['engine_temp_c']}°C - Comb: {data['fuel_level_percent']}% - Odo: {data['odometer_km']} km")
                    elapsed_real_time = time.time() - start_real_time
                    progress = (message_count / (num_messages_per_vehicle * num_vehicles)) * 100
                    print(f"Progreso: {progress:.1f}% - Mensajes enviados: {message_count} - Tiempo transcurrido: {elapsed_real_time:.1f}s")
            except Exception as e:
                print(f"Error enviando mensaje para {device_id} en {current_sim_time}: {e}")

        # Avanzar el tiempo simulado por el intervalo definido
        current_sim_time += timedelta(minutes=simulated_interval_minutes)

        # Pausa para cumplir con la duración real deseada
        # Solo duerme si aún no hemos superado el tiempo de simulación
        if current_sim_time < end_sim_time:
            time.sleep(delay_between_messages)

    print(f"\nSimulación completada!")
    print(f"Total de mensajes enviados: {message_count}")
    print(f"Tiempo real transcurrido: {time.time() - start_real_time:.1f} segundos")

except KeyboardInterrupt:
    print("\nDeteniendo generación de datos...")
finally:
    if 'client' in locals() and hasattr(client, 'connected') and client.connected:
        client.shutdown()
        print("Generación de datos completada y cliente desconectado.")
    elif 'client' in locals():
        print("Generación de datos completada.")
    else:
        print("El cliente IoT Hub no se inicializó.")