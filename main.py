from models import OrderLines


def main(request):
    data = request.get_json()
    print(data)

    response = OrderLines(
        data.get("start"),
        data.get("end"),
    ).run()

    print(response)
    return response
