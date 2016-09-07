import sys
import os
import csv
import json
import pyrebase

writer = csv.writer(sys.stdout, delimiter='\t')

if '--help' in sys.argv or len(sys.argv) < 2:
  print('Usage:')
  print('  python -m bikedatacollector list')
  print('  python -m bikedatacollector fetch jobid')
  print('  python -m bikedatacollector parse filename')
  sys.exit(0)

task = sys.argv[1].strip().lower()

if task in ('fetch', 'list'):

  PWD = os.path.dirname(os.path.realpath(__file__))
  with open('{}/../bikedatacollector-config.json'.format(PWD)) as f:
    config = json.load(f)
  config['serviceAccount'] = PWD + "/" + config['serviceAccount']

  firebase = pyrebase.initialize_app(config)

  db = firebase.database()

  runs = db.shallow().get()

  if task == 'list':
      print('\n'.join(runs.pyres))
      sys.exit(0)

  job_name = sys.argv[2].strip()
  if not job_name in runs.pyres:
      sys.stderr.write("{} not found in record list\n".format(job_name))
      sys.exit(1)

  job_data = db.child(job_name).get()

  print(json.dumps(job_data.val(), indent=2, ensure_ascii=False))

elif task == 'parse':
  with open(sys.argv[2]) as f:
    j = json.load(f)

  if len(sys.argv) >= 4:
    out_dir = os.path.normpath(sys.argv[3])
  else:
    out_dir = os.getcwd()

  version = 'v1'
  for k in j:
    for expected_key in ('msg', 'timestamp', 'coordTimestamp', 'coord', 'horizontalAccuracy'):
      if expected_key not in j[k]:
        version = 'v2'
    if version != 'v1':
      break


  sys.stderr.write('detected record format {}\n'.format(version))

  if version == 'v1':
    print('skipping v1 file')
    sys.exit(1)
    locations = {}
    ranges = []

    for k in j:
      item = j[k]
      locations[item.get('coordTimestamp')] = item.get('coord')
    locations = [ (t, locations[t]) for t in sorted(map(lambda x: float(x), locations.keys()))]

    line = {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "LineString",
        "coordinates": []
      }
    }

    for l in locations:
      line['geometry']['coordinates'].append(l[1])

    fc = {
      "type": "FeatureCollection",
      "features": [ line ]
    }

    for k in j:
      item = j[k]
      fc['features'].append({
        "type": "Point",
        "properties": { "distance": item['msg'] },
        "geometry": item['coord']
      })
      writer.writerow([item['timestamp'], item['msg']])

  if version == 'v2':
    # assemble GPS points
    locations = [ i for (k, i) in j.items() if 'coord' in i ]
    locations.sort(key=lambda x: x['timestamp'])

    # grab non-GPS points
    messages = [ i for (k, i) in j.items() if 'msg' in i and ':' not in i.get('msg') ]
    for i in range(0, len(messages)):
      parts = messages[i]['msg'].split('/')
      messages[i]['deviceTimestamp'] = float(parts.pop(0)) / 1000.0
      for sensor_i in range(len(parts)):
        messages[i]['sensor' + str(sensor_i)] = int(parts[sensor_i])
    messages.sort(key=lambda x: x['timestamp'])

    # find shortest gap between timestamps
    # measures gap b/w phone & uc is about 0.001%
    min_dist_pair = sorted([ (m['timestamp'] - m['deviceTimestamp'], m['timestamp'], m['deviceTimestamp']) for m in messages ], key=lambda x: x[0])[0]
    device_delta = min_dist_pair[1] - min_dist_pair[2]

    # adjust uC timestamps with delta
    for i in range(0, len(messages)):
      if 'deviceTimestamp' in messages[i]:
        messages[i]['adjustedDeviceTimestamp'] = messages[i]['deviceTimestamp'] + device_delta

    # record distances to JSON file
    with open(os.path.normcase('{}/{}-sonar.json'.format(out_dir, os.path.basename(sys.argv[2]).replace('.json', ''))), 'w') as json_f:
      json.dump(list(map(lambda x: (x['adjustedDeviceTimestamp'], x['sensor0'], x['sensor1']), [m for m in messages if 'msg' in m])), json_f, indent=2)

    # create geoJSON
    fc = {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {
            "timestamp": list(map(lambda x: x['timestamp'], locations)),
            "horizontalAccuracy": list(map(lambda x: x['horizontalAccuracy'], locations))

          },
          "geometry": {
            "type": "LineString",
            "coordinates": list(map(lambda x: x['coord'], locations))
          }
        }
      ]
    }

    with open(os.path.normcase('{}/{}.geojson'.format(out_dir, os.path.basename(sys.argv[2]).replace('.json', ''))), 'w') as json_f:
      json.dump(fc, json_f, indent=2)

  else:
    sys.stderr.write('Nothing to do, quitting\n')
    sys.exit(1)

