import requests
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport


def main():
    sample_transport = RequestsHTTPTransport(
        url='http://63.32.106.84:8000/graphql/',
        use_json=True,
        headers={
            "Content-type": "application/json",
        },
        verify=False
    )

    client = Client(
        retries=3,
        transport=sample_transport,
        fetch_schema_from_transport=True
    )

    response = requests.get("http://167.172.39.249:8000/api/images/")
    inspo_ids = [r['id'] for r in response.json()]

    query = '''
        {{
          allFiles(take : {}){{
            id
            title
            categories{{
              id
            }}
          }}
        }}
    '''
    graphica_ids = []

    objects = client.execute(gql(query.format(1500)))

    for obj in objects['allFiles']:
        if len(graphica_ids) == len(inspo_ids):
            break

        if len(obj['categories']) == 0 or obj['title'] == "":
            continue

        graphica_ids.append(obj['id'])

    # print(inspo_ids)
    # print(graphica_ids)
    with open('object_id_map.txt', 'w') as f:
        for i in range(len(inspo_ids)):
            print(inspo_ids[i], graphica_ids[i], sep=' ', end='\n', file=f)


if __name__ == '__main__':
    main()
