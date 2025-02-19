from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.enums import ParseMode
from aiogram import exceptions
import asyncio
import requests as rq
from bs4 import BeautifulSoup
import pandas as pd



BasePath = str(input('Set base path: ')) + r'/imba/imba.csv'
UsersPath = str(input('Set users path: ')) + r'/imba/users.csv'
BasePath = BasePath.replace('"', '')
UsersPath = UsersPath.replace('"', '')
SleepTime = 7200
#7200secs == 2h
API_TOKEN = str(input('Set bot API: '))
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()



def get_all_users():
    try:
        df = pd.read_csv(UsersPath, header=None, names=['user_id'])
        return df['user_id'].tolist()
    except FileNotFoundError:
        print(f"Файл {UsersPath} не найден.")
        return []
    except Exception as e:
        print(f"Ошибка при чтении файла {UsersPath}: {e}")
        return []

def save_users(user_ids):
    try:
        df = pd.DataFrame(user_ids, columns=['user_id'])
        df.to_csv(UsersPath, index=False, header=False)
        print("Список пользователей успешно обновлен.")
    except Exception as e:
        print(f"Ошибка при записи в файл {UsersPath}: {e}")

def add_user(user_id):
    try:
        df = pd.read_csv(UsersPath, header=None, names=['user_id'])
        if user_id not in df['user_id'].values:
            new_row = pd.DataFrame({'user_id': [user_id]})
            df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(UsersPath, index=False, header=False)
            print(f"Пользователь {user_id} добавлен.")
    except FileNotFoundError:
        df = pd.DataFrame({'user_id': [user_id]})
        df.to_csv(UsersPath, index=False, header=False)
        print(f"Файл {UsersPath} создан. Пользователь {user_id} добавлен.")
    except Exception as e:
        print(f"Ошибка при добавлении пользователя {user_id}: {e}")

async def parcer():
    while True:
        r = rq.get('https://imba.shop/catalog/energetiki')
        if r.status_code != 200:
            print('Сайт недоступен!')
        else:
            print('Site parcing ... OK')
            rowdata = BeautifulSoup(r.text, 'html.parser')

            data = rowdata.body.find('div', id="product-category-product")
            items = data.find_all('div', class_="col product-layout product-grid")
            count = len(items)
            link, name, price = [], [], []
            for item in items:
                if item.find('div', class_="product-thumb product-thumb__sold-out") == None:
                    itembody = item.find('div', class_='product-thumb__body')
                    if itembody.find('div', class_='product-thumb__price-special') != None:
                        itemsale = itembody.find('div', class_='product-thumb__price-special').string
                        itemsale = itemsale.replace(' ', '')
                        itemsale = int(itemsale)
                        itemfullprice = itembody.find('div', class_='product-thumb__price-base').string
                        itemfullprice = itemfullprice.replace(' ', '')
                        itemfullprice = int(itemfullprice)
                        percentprice = 1 - (itemsale / itemfullprice)
                        if percentprice > 0.25:
                            link.append(itembody.find('a', class_='product-thumb__name')['href'])
                            name.append(itembody.find('a', class_='product-thumb__name').string)
                            price.append(itemsale)
                    else:
                        itemfullprice = itembody.find('div', class_='product-thumb__price').string
                        itemfullprice = itemfullprice.replace(' ', '')
                        itemfullprice = int(itemfullprice)
                        if itemfullprice < 1200:
                            link.append(itembody.find('a', class_='product-thumb__name')['href'])
                            name.append(itembody.find('a', class_='product-thumb__name').string)
                            price.append(itemfullprice)



            forcsv = {'Link': link, 'Name': name, 'Price': price}
            forcsv = pd.DataFrame(forcsv)

            try:
                oldcsv = pd.read_csv(BasePath)
            except FileNotFoundError:
                oldcsv = {'Link': link, 'Name': name, 'Price': price}
                oldcsv = pd.DataFrame(oldcsv)

            new = forcsv['Name'].tolist()
            old = oldcsv['Name'].tolist()

            if new != old:
                await broadcast_message("Хей! \nНа сайте изменился список дешёвых энергосов.\nТебя может это заинтересовать!")

            forcsv.to_csv(BasePath, index=False)
            print(link, '\n', name, '\n', price)
        await asyncio.sleep(SleepTime)

def get_offers():
    df = pd.read_csv(BasePath)
    offers = df.to_dict('records')
    return offers

async def broadcast_message(message_text: str):
    users = get_all_users()
    print(users)
    active_users = []
    for user_id in users:
        try:
            await bot.send_message(user_id, message_text)
            active_users.append(user_id)
        except exceptions.TelegramForbiddenError:
            print(f"Бот заблокирован пользователем {user_id}. Удаляем из списка.")
        except exceptions.TelegramRetryAfter as e:
            print(f"Flood limit exceeded. Sleep {e.retry_after} seconds.")
            await asyncio.sleep(e.retry_after)
            await bot.send_message(user_id, message_text)
            active_users.append(user_id)
        except exceptions.TelegramAPIError as e:
            print(f"Не удалось отправить сообщение пользователю {user_id}: {e}")
    save_users(active_users)
async def show_offers_button(message: types.Message):
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="Показать предложения"))
    keyboard = builder.as_markup(resize_keyboard=True)
    await message.answer("Нажми кнопку, чтобы увидеть доступные предложения!", reply_markup=keyboard)

@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    user_name = message.from_user.first_name
    user_id = message.from_user.id
    add_user(user_id)
    await message.answer(f"Привет, {user_name}!\n Это неофициальный бот, который создан для одной задачи - вылавливание энергосиков Imba Energy по низкой цене.\n Периодически бот будет отправлять сообщения об изменении списка выгодных лотов.")
    await show_offers_button(message)

@dp.message(lambda message: message.text == "Показать предложения")
async def show_offers(message: types.Message):
    offers = get_offers()
    if len(offers) > 0 :
        response = "Доступные предложения:\n\n"
        for offer in offers:
            response += f"{offer['Name']}: {offer['Price']}р.\n {offer['Link']}\n\n\n"
        await message.answer(response)
    else:
        await message.answer("Халява пока что отсутствует\nЖдём выгодных предложений)\n\nПока ждёшь снижение цен на официальном сайте, можешь глянуть страницы на маркетплейсах" +
                             " (частенько на них цены вкуснее)\n\nOzon:\nhttps://www.ozon.ru/seller/imba-105758/products/?miniapp=seller_105758\n\n" +
                             "Yandex.Market:\nhttps://market.yandex.ru/business--imba/1017902")

async def main():
    asyncio.create_task(parcer())
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())