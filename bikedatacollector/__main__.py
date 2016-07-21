import sys
import os
import csv
import json
import pyrebase

writer = csv.writer(sys.stdout)

if '--help' in sys.argv or len(sys.argv) < 2:
  print('Usage:')
  print('  python -m bikedatacollector list')
  print('  python -m bikedatacollector fetch jobid')
  print('  python -m bikedatacollector geojson filename')
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

  job_name = sys.argv[1].strip()
  if not job_name in runs.pyres:
      sys.stderr.write("{} not found in record list\n".format(job_name))
      sys.exit(1)

  job_data = db.child(job_name).get()

  print(json.dumps(job_data.val(), indent=2, ensure_ascii=False))

elif task == 'geojson':
  with open(sys.argv[2]) as f:
    j = json.load(f)

  version = 'v1'
  for k in j:
    for expected_key in ('msg', 'timestamp', 'coordTimestamp', 'coord', 'horizontalAccuracy'):
      if expected_key not in j[k]:
        version = 'v2'
        break

  sys.stderr.write('detected record format {}\n'.format(version))

  if version == 'v1':
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

    # print(json.dumps(fc))


  else:
    pass

