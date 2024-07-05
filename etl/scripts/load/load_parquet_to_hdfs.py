import subprocess

def load_to_hdfs():
    script_path = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/scripts/load/hdfsbash.sh'
    try:
        # Run the script
        result = subprocess.run(['/bin/bash', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Check if the script ran successfully
        if result.returncode == 0:
            print(f"Script output:\n{result.stdout}")
        else:
            print(f"Script error:\n{result.stderr}")
    except Exception as e:
        print(f"An error occurred: {e}")


