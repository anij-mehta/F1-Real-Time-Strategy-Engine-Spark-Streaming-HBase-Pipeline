import fastf1
import socket
import json
import time
import pandas as pd

def start_f1_stream():
    host = '0.0.0.0'
    port = 9999
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(1)
    
    print(f"📡 Strategy Engine Streamer Active on {port}...")
    conn, addr = server_socket.accept()

    session = fastf1.get_session(2024, 'Bahrain', 'R')
    session.load(telemetry=False, weather=False)
    
    all_laps = session.laps.sort_values(by='Time')

    try:
        for i in range(len(all_laps)):
            row = all_laps.iloc[i]
            
            # Gap Calculation
            if i > 0:
                actual_gap = round(abs(row['Time'].total_seconds() - all_laps.iloc[i-1]['Time'].total_seconds()), 3)
            else:
                actual_gap = 0.0

            # Building the Strategy-Focused Payload
            data = {
                "Driver": str(row.get('Driver', 'UNK')),
                "LapNumber": int(row.get('LapNumber', 0)),
                "LapTime": str(row.get('LapTime', '0')).split()[-1],
                "GapToAhead": actual_gap,
                "Compound": str(row.get('Compound', 'UNKNOWN')),
                "TyreLife": float(row.get('TyreLife', 0.0)),
                "FreshTyre": str(row.get('FreshTyre', 'True')), # Strategy signal
                "Position": int(row.get('Position', 0)),        # For tracking fall-off
                "IsPitStop": not pd.isna(row.get('PitOutTime')) # Detects the undercut attempt
            }
            
            message = json.dumps(data) + "\n"
            conn.send(message.encode('utf-8'))
            print(f"Sent: {data['Driver']} | Lap: {data['LapNumber']} | Pit: {data['IsPitStop']}")
            
            time.sleep(0.2) 
            
    except Exception as e:
        print(f"❌ Stream Error: {e}")
    finally:
        conn.close()
        server_socket.close()

if __name__ == "__main__":
    start_f1_stream()