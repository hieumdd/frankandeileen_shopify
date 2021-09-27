from models import Orders


def main(request):
    data = request.get_json()
    print(data)

    response = Orders(
        data.get("start"),
        data.get("end"),
    ).run()

    print(response)
    return response
