import Algorithmia

input = {
  "dataset_key": "bryon/odin-2015-2016",
  "query": "SELECT Country, `Overall subscore` FROM `ODIN-2015-2016-standardized` WHERE Year=? AND Elements=? ORDER BY `Overall subscore` DESC LIMIT 5",
  "query_type": "sql",
  "parameters": [
    "2016",
    "Population & Vital Statistics"
  ]
}
client = Algorithmia.client('simCZp/XX67pT0f0RlFTnEsf/qg1')
algo = client.algo('datadotworld/query/0.2.0')
print(algo.pipe(input).result)
