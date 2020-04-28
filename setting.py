def get_headers():
    headers = {}
    with open('headers.txt', 'r') as f:
        for l in f:
            l = l.strip(' \t\n')
            if l:
                n, v = l.split(': ', 1)
                headers[n] = v
    return headers


def get_proxies():
    return {
        'http': "socks5://localhost:9150",
        'https': "socks5://localhost:9150"
    }
