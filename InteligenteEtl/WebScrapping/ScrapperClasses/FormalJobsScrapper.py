from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
# Set up the WebDriver
driver = webdriver.Chrome()

def click_voltar_icon(driver):
    # Function to click the 'voltar icon' button
    try:
        voltar_icon = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div#lista-base-busca .voltar.icon-left-big'))
        )
        voltar_icon.click()
    except Exception as e:
        print(f"Error clicking on voltar icon: {e}")

try:
    # Open the webpage
    driver.maximize_window()
    driver.get("https://www.ibge.gov.br/apps/snig/v1/?loc=0&cat=-1,-2,-3,128&ind=4732")

    # Wait until the local-selector div is present and click the button inside it
    local_selector_div = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'local-selector'))
    )
    time.sleep(1.5)
    local_selector_div = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'local-selector'))
    )
    step_div = local_selector_div.find_element(By.CLASS_NAME, 'step')
    step_div.click()

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
    ul_list = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '#local-selector .body-list'))
    )

    # Debugging: Print if ul_list is found
    if ul_list:
        print("UL list found.")

    print(ul_list.text)

    municipio_li = None
    li_elements = ul_list.find_elements(By.TAG_NAME, 'li')
    for li in li_elements:
        if "Municípios" in li.text:
            municipio_li = li
            break
    print(municipio_li.text)
    
    if municipio_li:
         # Click on the label inside the list item
         span_inside_li = municipio_li.find_element(By.TAG_NAME, 'span')
         span_inside_li.click()

         index_li: int = 0
         while True:
            # Wait for the UL with class 'list body-list' to appear
            next_ul = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.list.body-list'))
            )

            option_li_elements = next_ul.find_elements(By.CSS_SELECTOR, 'li.option.with-child')
            option_li = option_li_elements[index_li]
            print(option_li.text)
            try:
                    # Click on the 'local' tag inside each 'li'
                    local_tag = option_li.find_element(By.CLASS_NAME, 'local')
                    local_tag.click()
                    time.sleep(1)

                    # Re-find the 'ul' element to refresh the context
                    next_ul = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.list.body-list'))
                    )

                    first_li = next_ul.find_element(By.CSS_SELECTOR, 'li.option')
                    input_tag = first_li.find_element(By.CSS_SELECTOR, 'div.input > input')
                    input_tag.click()
                    time.sleep(2)

                    # Click the 'voltar icon' button to go back to the previous state
                    click_voltar_icon(driver)
                    index_li+=1
                      # Break inner loop to re-evaluate the list

            except Exception as e:
                    print(f"Error clicking on local tag: {e}")
    else:
        print("Element with text 'Municípios' not found.")
    time.sleep(10)



finally:
    # Close the browser
    driver.quit()