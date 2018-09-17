import Algorithmia

input = {
  "image": "data://deeplearning/example_data/lincoln.jpg"
}
client = Algorithmia.client('simCZp/XX67pT0f0RlFTnEsf/qg1')
algo = client.algo('deeplearning/ColorfulImageColorization/1.1.13')
print(algo.pipe(input).result)
