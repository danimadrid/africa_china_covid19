import justext
import requests
import requests_random_user_agent

def get_web_texts(url):
  try: 
    response = requests.get(url)
    paragraphs = justext.justext(response.content, justext.get_stoplist("English"))
    response.close()
    for paragraph in paragraphs:
      if not paragraph.is_boilerplate:
        print(paragraph.text)
  except:
    print(" ")
