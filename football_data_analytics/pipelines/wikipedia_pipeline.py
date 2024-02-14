def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        return response.text

    except requests.RequestException as e:
        print(f"an error occured: {e}")

def get_wikipedia_data(html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')