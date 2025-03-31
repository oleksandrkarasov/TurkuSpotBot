import telebot
from telebot import types
import os
import csv
import logging
import time
import datetime
import threading
import random
import hashlib
import json
import sqlite3
import queue

# Set up logging
logging.basicConfig(level=logging.INFO)

# Local directory to store files
local_storage_dir = 'XXX'  

# Telegram bot token
TOKEN = 'XXX'  

bot = telebot.TeleBot(TOKEN, threaded=True)

data_file = 'city_issue_data.csv'

# Dictionaries to store user data during conversation
user_data = {}

# File to store the last used id
last_id_file = os.path.join(local_storage_dir, 'last_id.txt')

# Initialize last_id
if os.path.exists(last_id_file):
    with open(last_id_file, 'r') as f:
        last_id = int(f.read())
else:
    last_id = 0

# Initialize a lock for thread safety
last_id_lock = threading.Lock()



def generate_anonymous_id(user_id):
    """
    Generate a consistent anonymous ID for a Telegram user_id.
    Format: AdjectiveNoun123
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        str: Anonymous ID string
    """
    # Random nickname generation data
    adjectives = [
        'Agile', 'Ancient', 'Brave', 'Bright', 'Calm', 'Charming',
        'Clever', 'Cool', 'Creative', 'Cute', 'Daring', 'Eager', 
        'Energetic', 'Friendly', 'Funny', 'Gentle', 'Graceful', 
        'Happy', 'Helpful', 'Honest', 'Kind', 'Lively', 'Lucky', 
        'Mysterious', 'Nice', 'Nimble', 'Peaceful', 'Playful',
        'Proud', 'Quick', 'Quiet', 'Smart', 'Smooth', 'Soft', 
        'Strong', 'Swift', 'Thoughtful', 'Warm', 'Wild', 'Wise'
    ]
    nouns = [
        'Antelope', 'Badger', 'Bear', 'Beaver', 'Bee', 'Butterfly',
        'Cat', 'Chicken', 'Deer', 'Dog', 'Dolphin', 'Duck', 'Eagle', 
        'Elephant', 'Fox', 'Frog', 'Giraffe', 'Goat', 'Hamster', 
        'Hawk', 'Hedgehog', 'Horse', 'Koala', 'Lion', 'Lizard', 
        'Monkey', 'Moose', 'Mouse', 'Owl', 'Panda', 'Parrot',
        'Penguin', 'Rabbit', 'Raccoon', 'Seal', 'Sheep', 'Squirrel', 
        'Swan', 'Tiger', 'Turtle', 'Wolf', 'Zebra'
    ]
    
    # Create a hash of the user_id to ensure the same user always gets the same anonymous ID
    # Use SHA-256 for good distribution
    hash_obj = hashlib.sha256(str(user_id).encode())
    hash_hex = hash_obj.hexdigest()
    
    # Use the hash to seed the random generator for consistent results
    random.seed(hash_hex)
    
    # Select a random adjective and noun
    adjective = random.choice(adjectives)
    noun = random.choice(nouns)
    
    # Generate a number between 0 and 999
    number = random.randint(0, 999)
    
    # Create and return the anonymous ID
    anonymous_id = f"{adjective}{noun}{number:03d}"
    
    # Reset the random seed to avoid affecting other random operations
    random.seed()
    
    return anonymous_id

# Messages dictionary


messages = {
    'en': {
        'welcome': "Welcome to TurkuSpot Bot! 🏙️\n\nThis service allows you to report environmental issues or suggest improvements in Turku. Your input helps make our city a cleaner and better place to live. \n\n ⚠️In case you expect someone's health to be endangered or observe severe pollution in natural waters or on the ground, immediately contact 112!",
        'select_language': "Please select your preferred language:",
        'language_selected': "Language set to English.",
        'consent_prompt': "Before we begin, we need your consent to collect the information you provide, including your location data. This information will be used to improve city services and community infrastructure. Your personal information is not collected.",
        'consent_options': ["I agree", "I do not agree"],
        'consent_given': "Thank you for agreeing to participate!",
        'consent_declined': "We understand your decision. You can come back anytime if you change your mind.\n\nTo restart the bot, press the button below.",
        'restart_button': "Restart",
        # Add these entries to each language in the messages dictionary
        'modify_location': "📍 Location",
        'modify_action': "📋 Issue/Improvement",
        'modify_socio': "👤 Personal background",
        'select_action': "What would you like to do?",
        'action_options': ["Report an issue", "Suggest an improvement"],
        'your_response': "Your response:",
        'select_questions_to_modify': "Which parts of your response would you like to modify?",
        'invalid_selection': "Invalid selection. Please try again.",
        'confirm_responses': "Please review your responses above. Do you confirm them or would like to modify anything?",
        'modify_responses': "I want to modify my responses",
        'confirm_submission': "I confirm my responses",
        'issue_list_prompt': "What type of issue would you like to report?",
        'issue_list': [
            "Smoke from fire or burning",
            "Strong smell (e.g. sewage)",
            "Air pollution (e.g. street dust)",
            "Flower pollen",
            "Oil, paint, or chemical waste",
            "Algal bloom or green water",
            "Illegal dumping",
            "Litter in natural areas",
            "Overflowing public bins",
            "Vandalism (e.g. broken utilities)",
            "Other (please specify)"
        ],
        'improvement_list_prompt': "What improvement would you like to suggest?",
        'improvement_list': [
            "Cleaner air in this area",
            "Better water quality here",
            "Better maintenance in this area",
            "Less noise at this location",
            "Less light pollution at night",
            "More shade or trees in this spot",
            "Public drinking fountain needed",
            "Cleaner green space at this location",
            "Less vehicle exhaust in this area",
            "Reducing odours in the area",
            "Other (please specify)"
        ],
        'location_request': "Please send the location of the issue or where you would like to see an improvement.\n\nTo do this:\n1. Tap the attachment icon (📎) in the message bar.\n2. Select 'Location'.\n3. Move the map to the desired location.\n4. Tap 'Send this location'.\n\nAlternatively, you can type the address or describe the location.",
        'additional_info_prompt': "Would you like to provide any additional details? This is optional, but helpful.",
        'skip_button': "Skip",
        'other_option': "Other (please specify)",
        'specify_other': "Please specify:",
        'thank_you': "Thank you! Your submission has been received and will be reviewed.",
        'submission_summary': "Here's a summary of your submission:",
        'done_button': "Done",
        'submission_received': "Thank you for your contribution to making our city better! Your submission has been received.",
        'submit_another': "Would you like to submit another location?",
        'submit_another_options': ["Yes, submit another", "No, I'm done"],
        'location_received': "Location received",
        'please_send_location': "Invalid input. Please send the location as instructed, or type the address/description of the location.",
        'free_text_added': "Your input has been added. You can select more options, add more text, or press Done when finished.",
        'please_select_at_least_one': "Please select at least one option or type your own.",
        'error_occurred': "An error occurred. Please try again later.",
        'socioeconomic_intro': "Would you like to share some information about yourself? This helps us understand who uses our services.",
        'socioeconomic_options': ["Yes, I'll share", "No, skip this part"],
        'labels': {
            'location': 'Location',
            'issue_type': 'Issue type',
            'improvement_type': 'Improvement type',
            'age': 'Age',
            'gender': 'Gender',
            'occupation': 'Occupation',
            'time_in_turku': 'Time living in Turku'
        },
        'age_question': "Please select your age group:",
        'age_options': [
            "18-25", "26-40", "41-60", "Above 60", "Prefer not to disclose"
        ],
        'gender_question': "Please select your gender:",
        'gender_options': [
            "Male", "Female", "Other", "Prefer not to disclose"
        ],
        'occupation_question': "Please select your occupational status:",
        'occupation_options': [
            "Working", "Not working", "Student", "Retired", "Military service", "Other", "Prefer not to disclose"
        ],
        'time_in_turku_question': "How long have you lived in Turku?",
        'time_in_turku_options': [
            "Less than 1 year", "1-3 years", "3-10 years", "10-20 years", "My whole life", "I don't live in Turku", "Prefer not to disclose"
        ]
    },
    'fi': {
        'welcome': "Tervetuloa TurkuSpot Botiin! 🏙️\n\nTämän palvelun avulla voit ilmoittaa ympäristöasioista tai ehdottaa parannuksia Turkuun. Panoksesi auttaa tekemään kaupungistamme puhtaamman ja paremman paikan asua. \n\n⚠️ Jos epäilet, että jonkun terveys on vaarassa tai havaitset vakavaa saastumista luonnon vesissä tai maaperässä, ota välittömästi yhteyttä numeroon 112!",
        'select_language': "Valitse haluamasi kieli:",
        'language_selected': "Kieleksi asetettu suomi.",
        'consent_prompt': "Ennen kuin aloitamme, tarvitsemme suostumuksesi kerätä antamiasi tietoja, mukaan lukien sijaintitietoja. Näitä tietoja käytetään kaupungin palvelujen ja yhteisön infrastruktuurin parantamiseen. Henkilökohtaisia tietojasi ei kerätä.",
        'consent_options': ["Hyväksyn", "En hyväksy"],
        'consent_given': "Kiitos osallistumisestasi!",
        'consent_declined': "Ymmärrämme päätöksesi. Voit palata milloin tahansa, jos muutat mieltäsi.\n\nKäynnistääksesi botin uudelleen, paina alla olevaa painiketta.",
        'restart_button': "Käynnistä uudelleen",
        'modify_location': "📍 Sijainti",
        'modify_action': "📋 Ongelma/Parannus", 
        'modify_socio': "👤 Henkilökohtaiset tiedot",
        'select_action': "Mitä haluaisit tehdä?",
        'action_options': ["Ilmoita ongelmasta", "Ehdota parannusta"],
        'your_response': "Vastauksesi:",
        'select_questions_to_modify': "Mitä osaa vastauksestasi haluat muokata?",
        'invalid_selection': "Virheellinen valinta. Yritä uudelleen.",
        'confirm_responses': "Tarkista vastauksesi yllä. Vahvistatko ne vai haluatko muokata jotain?",
        'modify_responses': "Haluan muokata vastauksia",
        'confirm_submission': "Vahvistan vastaukseni",
        'issue_list_prompt': "Minkä tyyppisestä ongelmasta haluaisit ilmoittaa?",
        'issue_list': [
            "Savuhaitta, palaminen",
            "Voimakas haju (esim. viemäri)",
            "Ilmansaaste (esim. katupöly)",
            "Kukkien siitepöly",
            "Öljyä, maalia tai kemikaaleja",
            "Leväkukinto tai vihreä vesi",
            "Laiton kaatopaikka",
            "Roskia luonnonalueilla",
            "Ylitäyttyneet julkiset roska-astiat",
            "Vandalismi",
            "Muu (täsmennä)"
        ],
        'improvement_list_prompt': "Mitä parannusta haluaisit ehdottaa?",
        'improvement_list': [
            "Puhtaampi ilma alueella",
            "Parempi veden laatu alueella",
            "Kunnossapidon lisääminen alueella",
            "Hiljaisempi ympäristö tässä paikassa",
            "Vähemmän valosaastetta alueella",
            "Enemmän varjoa tai puita tälle alueelle",
            "Julkinen vedenjakelulu alueelle",
            "Puhtaampi viheralue tässä paikassa",
            "Vähemmän ajoneuvoja alueella",
            "Hajuhaittojen vähentäminen alueella",
            "Muu (täsmennä)"
        ],
        'location_request': "Lähetä ongelman sijainti tai paikka, jossa haluaisit nähdä parannuksen.\n\nTee tämä näin:\n1. Napauta liitepainiketta (📎) viestipalkissa.\n2. Valitse 'Sijainti'.\n3. Siirrä kartta haluttuun paikkaan.\n4. Napauta 'Lähetä tämä sijainti'.\n\nVaihtoehtoisesti voit kirjoittaa osoitteen tai kuvailla sijainnin.",
        'additional_info_prompt': "Haluaisitko antaa lisätietoja? Tämä on vapaaehtoista, mutta hyödyllistä.",
        'skip_button': "Ohita",
        'other_option': "Muu (täsmennä)",
        'specify_other': "Täsmennä:",
        'thank_you': "Kiitos! Lähetyksesi on vastaanotettu ja sitä tarkastellaan.",
        'submission_summary': "Tässä on yhteenveto lähetyksestäsi:",
        'done_button': "Valmis",
        'submission_received': "Kiitos panoksestasi kaupunkimme parantamiseksi! Palautteesi on vastaanotettu.",
        'submit_another': "Haluaisitko lähettää toisen sijainnin?",
        'submit_another_options': ["Kyllä, lähetä toinen", "Ei, olen valmis"],
        'location_received': "Sijainti vastaanotettu",
        'please_send_location': "Virheellinen syöte. Lähetä sijainti ohjeiden mukaisesti tai kirjoita osoite/sijainnin kuvaus.",
        'free_text_added': "Syötteesi on lisätty. Voit valita lisää vaihtoehtoja, lisätä lisää tekstiä tai painaa Valmis, kun olet valmis.",
        'please_select_at_least_one': "Valitse vähintään yksi vaihtoehto tai kirjoita omasi.",
        'error_occurred': "Virhe tapahtui. Yritä myöhemmin uudelleen.",
        'socioeconomic_intro': "Haluaisitko jakaa joitain tietoja itsestäsi? Tämä auttaa meitä ymmärtämään, ketkä käyttävät palveluitamme.",
        'socioeconomic_options': ["Kyllä, jaan", "Ei, ohita tämä osa"],
        'labels': {
            'location': 'Sijainti',
            'issue_type': 'Ongelman tyyppi',
            'improvement_type': 'Parannuksen tyyppi',
            'age': 'Ikä',
            'gender': 'Sukupuoli',
            'occupation': 'Ammatti',
            'time_in_turku': 'Asuinaika Turussa'
        },
        'age_question': "Valitse ikäryhmäsi:",
        'age_options': [
            "18-25", "26-40", "41-60", "Yli 60", "En halua kertoa"
        ],
        'gender_question': "Valitse sukupuolesi:",
        'gender_options': [
            "Mies", "Nainen", "Muu", "En halua kertoa"
        ],
        'occupation_question': "Valitse työllisyystilanteesi:",
        'occupation_options': [
            "Työssäkäyvä", "Ei työssä", "Opiskelija", "Eläkeläinen", "Asepalveluksessa", "Muu", "En halua kertoa"
        ],
        'time_in_turku_question': "Kuinka kauan olet asunut Turussa?",
        'time_in_turku_options': [
            "Alle vuoden", "1-3 vuotta", "3-10 vuotta", "10-20 vuotta", "Koko elämäni", "En asu Turussa", "En halua kertoa"
        ]
    },
    'sv': {
        'welcome': "Välkommen till TurkuSpot Bot! 🏙️\n\nDenna tjänst låter dig rapportera miljöproblem eller föreslå förbättringar i Åbo. Ditt bidrag hjälper till att göra vår stad till en renare och bättre plats att bo på. \n\n⚠️ Om du misstänker att någons hälsa är i fara eller observerar allvarlig förorening i naturvatten eller mark, kontakta omedelbart 112!",
        'select_language': "Välj önskat språk:",
        'language_selected': "Språk inställt på svenska.",
        'consent_prompt': "Innan vi börjar behöver vi ditt samtycke till att samla in information som du tillhandahåller, inklusive din platsdata. Denna information kommer att användas för att förbättra stadens tjänster och samhällsinfrastruktur. Din personliga information samlas inte in.",
        'consent_options': ["Jag godkänner", "Jag godkänner inte"],
        'consent_given': "Tack för ditt deltagande!",
        'consent_declined': "Vi förstår ditt beslut. Du kan returnera när som helst om du ändrar dig.\n\nOm du vill starta om botten klickar du på knappen nedan.",
        'restart_button': "Starta om",
        'modify_location': "📍 Plats",
        'modify_action': "📋 Problem/Förbättring",
        'modify_socio': "👤 Personlig bakgrund",
        'select_action': "Vad skulle du vilja göra?",
        'action_options': ["Rapportera ett problem", "Föreslå en förbättring"],
        'your_response': "Ditt svar:",
        'select_questions_to_modify': "Vilka delar av ditt svar vill du ändra?",
        'invalid_selection': "Ogiltigt val. Försök igen.",
        'confirm_responses': "Vänligen granska dina svar ovan. Bekräftar du dem eller vill du ändra något?",
        'modify_responses': "Jag vill ändra mina svar",
        'confirm_submission': "Jag bekräftar mina svar",
        'issue_list_prompt': "Vilken typ av problem vill du rapportera?",
        'issue_list': [
            "Rökskada, förbränning",
            "Stark lukt (t.ex. rök, avloppsvatten)",
            "Luftföroreningar (t.ex. gatudamm)",
            "Blomma pollen",
            "Olja, färg eller kemikalier",
            "Algblomning eller grönt vatten",
            "Olaglig dumpning (t.ex. sopsäckar)",
            "Skräp i naturområden",
            "Överfulla allmänna papperskorgar",
            "Vandalism",
            "Annat (vänligen specificera)"
        ],
        'improvement_list_prompt': "Vilken förbättring skulle du vilja föreslå?",
        'improvement_list': [
            "Renare luft i den här området",
            "Bättre vattenkvalitet i området",
            "Ökat underhåll i området",
            "Tystare miljö på denna plats",
            "Mindre ljusföroreningar i området",
            "Mer skugga eller träd på denna plats",
            "Allmän vattenförsörjning till området",
            "Renare grönområde på denna plats",
            "Färre (motor)fordon i området",
            "Minskning av luktföroreningar i området",
            "Annat (vänligen specificera)"
        ],
        'location_request': "Vänligen skicka platsen för problemet eller där du skulle vilja se en förbättring.\n\nFör att göra detta:\n1. Tryck på bifogningsikonen (📎) i meddelandefältet.\n2. Välj 'Plats'.\n3. Flytta kartan till önskad plats.\n4. Tryck på 'Skicka denna plats'.\n\nAlternativt kan du skriva adressen eller beskriva platsen.",
        'additional_info_prompt': "Vill du lämna ytterligare information? Detta är frivilligt men hjälpsamt.",
        'skip_button': "Hoppa över",
        'other_option': "Annat (vänligen specificera)",
        'specify_other': "Vänligen specificera:",
        'thank_you': "Tack! Din anmälan har tagits emot och kommer att granskas.",
        'submission_summary': "Här är en sammanfattning av din anmälan:",
        'done_button': "Klar",
        'submission_received': "Tack för ditt bidrag till att göra vår stad bättre! Din anmälan har tagits emot.",
        'submit_another': "Vill du skicka in en annan plats?",
        'submit_another_options': ["Ja, skicka in en annan", "Nej, jag är klar"],
        'location_received': "Plats mottagen",
        'please_send_location': "Ogiltig inmatning. Vänligen skicka platsen enligt instruktionerna eller skriv adressen/beskrivning av platsen.",
        'free_text_added': "Din inmatning har lagts till. Du kan välja fler alternativ, lägga till mer text eller trycka på Klar när du är färdig.",
        'please_select_at_least_one': "Välj minst ett alternativ eller skriv ditt eget.",
        'error_occurred': "Ett fel inträffade. Försök igen senare.",
        'socioeconomic_intro': "Vill du dela med dig av information om dig själv? Detta hjälper oss att förstå vilka som använder våra tjänster.",
        'socioeconomic_options': ["Ja, jag delar", "Nej, hoppa över denna del"],
        'labels': {
            'location': 'Plats',
            'issue_type': 'Problemtyp',
            'improvement_type': 'Förbättringstyp',
            'age': 'Ålder',
            'gender': 'Kön',
            'occupation': 'Sysselsättning',
            'time_in_turku': 'Tid boende i Åbo'
        },
        'age_question': "Vänligen välj din åldersgrupp:",
        'age_options': [
            "18-25", "26-40", "41-60", "Över 60", "Föredrar att inte svara"
        ],
        'gender_question': "Vänligen välj ditt kön:",
        'gender_options': [
            "Man", "Kvinna", "Annat", "Föredrar att inte svara"
        ],
        'occupation_question': "Vänligen välj din sysselsättningsstatus:",
        'occupation_options': [
            "Arbetande", "Arbetslös", "Student", "Pensionerad", "Militärtjänst", "Annat", "Föredrar att inte svara"
        ],
        'time_in_turku_question': "Hur länge har du bott i Åbo?",
        'time_in_turku_options': [
            "Mindre än 1 år", "1-3 år", "3-10 år", "10-20 år", "Hela mitt liv", "Jag bor inte i Åbo", "Föredrar att inte svara"
        ]
    },
    'uk': {
        'welcome': "Ласкаво просимо до TurkuSpot Bot! 🏙️\n\nЦей сервіс дозволяє вам повідомляти про екологічні проблеми або пропонувати покращення в Турку. Ваш внесок допомагає зробити наше місто чистішим та кращим місцем для життя. \n\n⚠️ Якщо ви підозрюєте, що комусь загрожує небезпека для здоров'я або спостерігаєте серйозне забруднення природних вод чи ґрунтів — негайно зверніться за номером 112!",
        'select_language': "Будь ласка, оберіть вашу мову:",
        'language_selected': "Мову встановлено на українську.",
        'consent_prompt': "Перш ніж ми почнемо, нам потрібна ваша згода на збір інформації, яку ви надаєте, включаючи дані про ваше місцезнаходження. Ця інформація буде використана для покращення міських послуг та інфраструктури. Ваша особиста інформація не збирається.",
        'consent_options': ["Я погоджуюсь", "Я не погоджуюсь"],
        'consent_given': "Дякуємо за вашу згоду на участь!",
        'consent_declined': "Ми розуміємо ваше рішення. Ви можете повернутися будь-коли, якщо передумаєте.\n\nЩоб перезапустити бота, натисніть кнопку нижче.",
        'restart_button': "Перезапустити",
        'modify_location': "📍 Місцезнаходження",
        'modify_action': "📋 Проблема/Покращення",
        'modify_socio': "👤 Особиста інформація",
        'select_action': "Що б ви хотіли зробити?",
        'action_options': ["Повідомити про проблему", "Запропонувати покращення"],
        'your_response': "Ваша відповідь:",
        'select_questions_to_modify': "Які частини вашої відповіді ви хотіли б змінити?",
        'invalid_selection': "Невірний вибір. Спробуйте ще раз.",
        'confirm_responses': "Будь ласка, перегляньте ваші відповіді вище. Ви підтверджуєте їх чи хотіли б щось змінити?",
        'modify_responses': "Я хочу змінити мої відповіді",
        'confirm_submission': "Я підтверджую мої відповіді",
        'issue_list_prompt': "Про який тип проблеми ви хотіли б повідомити?",
        'issue_list': [
            "Дим від вогню або горіння",
            "Сильний запах (каналізація тощо)",
            "Забруднення повітря (вуличний пил тощо)",
            "Квітковий пилок",
            "Нафта, фарба або хімічні відходи",
            "Цвітіння водоростей або зелена вода",
            "Незаконне скидання сміття",
            "Сміття в природних зонах",
            "Переповнені громадські урни",
            "Вандалізм (наприклад, графіті)",
            "Інше (будь ласка, вкажіть)"
        ],
        'improvement_list_prompt': "Яке покращення ви хотіли б запропонувати?",
        'improvement_list': [
            "Чистіше повітря в цій зоні",
            "Краща якість води тут",
            "Покращення обслуговування цієї зони",
            "Менше шуму в цьому місці",
            "Менше світлового забруднення вночі",
            "Більше тіні або дерев у цьому місці",
            "Потрібний громадський питний фонтан",
            "Чистіший зелений простір",
            "Менше викидів транспорту в цій зоні",
            "Зменшення запахів у цій зоні",
            "Інше (будь ласка, вкажіть)"
        ],
        'location_request': "Будь ласка, надішліть місцезнаходження проблеми або де ви хотіли б бачити покращення.\n\nЩоб зробити це:\n1. Натисніть на іконку вкладення (📎) в панелі повідомлень.\n2. Виберіть 'Місцезнаходження'.\n3. Перемістіть карту до бажаного місця.\n4. Натисніть 'Надіслати це місцезнаходження'.\n\nАльтернативно, ви можете ввести адресу або описати місцезнаходження.",
        'additional_info_prompt': "Чи хотіли б ви надати додаткову інформацію? Це необов'язково, але корисно.",
        'skip_button': "Пропустити",
        'other_option': "Інше (будь ласка, вкажіть)",
        'specify_other': "Будь ласка, вкажіть:",
        'thank_you': "Дякуємо! Ваше подання було отримано і буде розглянуто.",
        'submission_summary': "Ось підсумок вашого подання:",
        'done_button': "Готово",
        'submission_received': "Дякуємо за ваш внесок у покращення нашого міста! Ваше подання було отримано.",
        'submit_another': "Бажаєте подати ще одне місцезнаходження?",
        'submit_another_options': ["Так, подати ще одне", "Ні, я закінчив/ла"],
        'location_received': "Місцезнаходження отримано",
        'please_send_location': "Невірне введення. Будь ласка, надішліть місцезнаходження, як вказано в інструкціях, або введіть адресу/опис місцезнаходження.",
        'free_text_added': "Ваше введення було додано. Ви можете вибрати більше варіантів, додати більше тексту або натиснути Готово, коли закінчите.",
        'please_select_at_least_one': "Будь ласка, виберіть принаймні один варіант або введіть свій власний.",
        'error_occurred': "Сталася помилка. Будь ласка, спробуйте пізніше.",
        'socioeconomic_intro': "Чи хотіли б ви поділитися деякою інформацією про себе? Це допомагає нам зрозуміти, хто користується нашими послугами.",
        'socioeconomic_options': ["Так", "Ні, пропустити"],
        'labels': {
            'location': 'Місцерозташування',
            'issue_type': 'Тип проблеми',
            'improvement_type': 'Тип покращення',
            'age': 'Вік',
            'gender': 'Стать',
            'occupation': 'Рід занять',
            'time_in_turku': 'Час проживання в Турку'
        },
        'age_question': "Будь-ласка, оберіть Вашу вікову групу:",
        'age_options': [
            "18-25", "26-40", "41-60", "Старше 60", "Надаю перевагу не вказувати"
        ],
        'gender_question': "Будь-ласка, оберіть Вашу стать:",
        'gender_options': [
            "Чоловіча", "Жіноча", "Інше", "Надаю перевагу не вказувати"
        ],
        'occupation_question': "Будь-ласка, вкажіть Ваш рід занять:",
        'occupation_options': [
            "Працюю", "Не працюю", "Студент", "На пенсії", "Військова служба", "Інше", "Надаю перевагу не вказувати"
        ],
        'time_in_turku_question': "Як довго Ви живете в Турку?",
        'time_in_turku_options': [
            "Менше року", "1-3 роки", "3-10 років", "10-20 років", "Все життя", "Я не живу в Турку", "Надаю перевагу не вказувати"
        ]
    }
}

privacy_links = {
    'en': {
        'privacy_notice': 'https://telegra.ph/TurkuSPOTs-Privacy-Notice---English-03-28',
        'participant_info': 'https://telegra.ph/Participant-Information-Sheet-for-TurkuSPOT-project-03-28'
    },
    'fi': {
        'privacy_notice': 'https://telegra.ph/TurkuSPOT-tutkimuksen-tietosuojaseloste--Suomi-03-28',
        'participant_info': 'https://telegra.ph/Osallistujan-tiedote-TurkuSPOT-projektille--Suomi-03-28'
    },
    'sv': {
        'privacy_notice': 'https://telegra.ph/TurkuSPOTs-Dataskyddsbeskrivning--Svenska-03-28',
        'participant_info': 'https://telegra.ph/Deltagarinformation-för-TurkuSPOT-projektet--Svenska-03-28'
    },
    'uk': {
        'privacy_notice': 'https://telegra.ph/Povіdomlennya-pro-konfіdencіjnіst-TurkuSPOT--Ukrainskoyu-03-28',
        'participant_info': 'https://telegra.ph/Іnformacіjnij-list-dlya-uchasnikіv-proyektu-TurkuSPOT--Ukrainskoyu-03-28'
    }
}

# Now let's add new menu options to the existing messages dictionary
# I'll only add the new entries to keep this concise
menu_messages = {
    'en': {
        'menu_options': "Main Menu - Please select an option:",
        'menu_report': "📝 Report Issue/Improvement",
        'menu_privacy': "🔒 Privacy Notice",
        'menu_info': "ℹ️ Participant Information",
        'menu_language': "🌐 Change Language",
        'back_to_menu': "↩️ Back to Main Menu",
        'privacy_notice_title': "🔒 Privacy Notice",
        'privacy_notice_link': "Click here to view the Privacy Notice:",
        'participant_info_title': "ℹ️ Participant Information Sheet",
        'participant_info_link': "Click here to view the Participant Information Sheet:"
    },
    'fi': {
        'menu_options': "Päävalikko - Valitse vaihtoehto:",
        'menu_report': "📝 Ilmoita ongelma/parannusehdotus",
        'menu_privacy': "🔒 Tietosuojaseloste",
        'menu_info': "ℹ️ Osallistujan tiedote",
        'menu_language': "🌐 Vaihda kieltä",
        'back_to_menu': "↩️ Takaisin päävalikkoon",
        'privacy_notice_title': "🔒 Tietosuojaseloste",
        'privacy_notice_link': "Napsauta tästä nähdäksesi tietosuojaselosteen:",
        'participant_info_title': "ℹ️ Osallistujan tiedote",
        'participant_info_link': "Napsauta tästä nähdäksesi osallistujan tiedotteen:"
    },
    'sv': {
        'menu_options': "Huvudmeny - Välj ett alternativ:",
        'menu_report': "📝 Rapportera problem/förbättring",
        'menu_privacy': "🔒 Dataskyddsbeskrivning",
        'menu_info': "ℹ️ Deltagarinformation",
        'menu_language': "🌐 Byt språk",
        'back_to_menu': "↩️ Tillbaka till huvudmenyn",
        'privacy_notice_title': "🔒 Dataskyddsbeskrivning",
        'privacy_notice_link': "Klicka här för att se dataskyddsbeskrivningen:",
        'participant_info_title': "ℹ️ Deltagarinformation",
        'participant_info_link': "Klicka här för att se deltagarinformationen:"
    },
    'uk': {
        'menu_options': "Головне меню - Будь ласка, виберіть опцію:",
        'menu_report': "📝 Повідомити про проблему/покращення",
        'menu_privacy': "🔒 Повідомлення про конфіденційність",
        'menu_info': "ℹ️ Інформаційний лист для учасників",
        'menu_language': "🌐 Змінити мову",
        'back_to_menu': "↩️ Повернутися до головного меню",
        'privacy_notice_title': "🔒 Повідомлення про конфіденційність",
        'privacy_notice_link': "Натисніть тут, щоб переглянути повідомлення про конфіденційність:",
        'participant_info_title': "ℹ️ Інформаційний лист для учасників",
        'participant_info_link': "Натисніть тут, щоб переглянути інформаційний лист для учасників:"
    }
}

# Update the messages dictionary with menu options
for lang in messages:
    if lang in menu_messages:
        # Add menu options to each language in the messages dictionary
        for key, value in menu_messages[lang].items():
            messages[lang][key] = value

# You can also keep the update_welcome_message function for potential future use
def update_welcome_message():
    for lang in messages:
        if lang in menu_messages:
            # Add menu options to each language in the messages dictionary
            for key, value in menu_messages[lang].items():
                messages[lang][key] = value


# Constants and configuration
DB_POOL_SIZE = 10  # Adjust based on expected concurrent users
db_file = '/scratch/project_2004147/telebot/turkubot.db'
db_pool = queue.Queue(maxsize=DB_POOL_SIZE)
db_lock = threading.Lock()

# Set up a separate logger for data flow
flow_logger = logging.getLogger('TurkuBotDataFlow')
flow_handler = logging.FileHandler('/scratch/project_2004147/telebot/data_flow.log')
flow_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
flow_logger.addHandler(flow_handler)
flow_logger.setLevel(logging.INFO)

# Random nickname generation data
adjectives = [
    'Agile', 'Ancient', 'Brave', 'Bright', 'Calm', 'Charming',
    'Clever', 'Cool', 'Creative', 'Cute', 'Daring', 'Eager', 
    'Energetic', 'Friendly', 'Funny', 'Gentle', 'Graceful', 
    'Happy', 'Helpful', 'Honest', 'Kind', 'Lively', 'Lucky', 
    'Mysterious', 'Nice', 'Nimble', 'Peaceful', 'Playful',
    'Proud', 'Quick', 'Quiet', 'Smart', 'Smooth', 'Soft', 
    'Strong', 'Swift', 'Thoughtful', 'Warm', 'Wild', 'Wise'
]
nouns = [
    'Antelope', 'Badger', 'Bear', 'Beaver', 'Bee', 'Butterfly',
    'Cat', 'Chicken', 'Deer', 'Dog', 'Dolphin', 'Duck', 'Eagle', 
    'Elephant', 'Fox', 'Frog', 'Giraffe', 'Goat', 'Hamster', 
    'Hawk', 'Hedgehog', 'Horse', 'Koala', 'Lion', 'Lizard', 
    'Monkey', 'Moose', 'Mouse', 'Owl', 'Panda', 'Parrot',
    'Penguin', 'Rabbit', 'Raccoon', 'Seal', 'Sheep', 'Squirrel', 
    'Swan', 'Tiger', 'Turtle', 'Wolf', 'Zebra'
]

def initialize_database():
    """Initialize the SQLite database with required tables."""
    with db_lock:
        try:
            conn = sqlite3.connect(db_file, check_same_thread=False)
            cursor = conn.cursor()

            # Set pragmas for better performance
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")

            # Create table for user submissions - using anonymized IDs
            create_submissions_table_query = '''
                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT,  -- This will store anonymized IDs, not real Telegram IDs
                    submission_type TEXT,
                    standard_selections TEXT,
                    custom_inputs TEXT,
                    latitude TEXT,
                    longitude TEXT,
                    venue_title TEXT,
                    venue_address TEXT,
                    additional_info TEXT,
                    timestamp TEXT
                );
            '''
            cursor.execute(create_submissions_table_query)

            # Create table for user preferences - using anonymized IDs
            # Adding language column with default English
            create_user_preferences_table = '''
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id TEXT PRIMARY KEY,  -- This will store anonymized ID, not real Telegram ID
                    consent BOOLEAN,
                    last_active TEXT,
                    age TEXT,
                    gender TEXT,
                    occupation TEXT,
                    time_in_turku TEXT,
                    language TEXT DEFAULT 'en'
                );
            '''
            cursor.execute(create_user_preferences_table)
            
            # Create table for user nicknames
            create_nicknames_table = '''
                CREATE TABLE IF NOT EXISTS user_nicknames (
                    telegram_id TEXT PRIMARY KEY,
                    nickname TEXT NOT NULL,
                    created_at TEXT
                );
            '''
            cursor.execute(create_nicknames_table)

            # Add indexes for better performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON submissions(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON submissions(timestamp)")

            conn.commit()
            conn.close()
            
            return True

        except Exception as e:
            logging.exception(f"Error initializing database: {e}")
            flow_logger.error(f"Database initialization failed: {e}")
            return False




# Database connection pool functions
def initialize_connection_pool():
    """Initialize the database connection pool."""
    for _ in range(DB_POOL_SIZE):
        try:
            conn = sqlite3.connect(db_file, check_same_thread=False)
            # Set pragmas for better performance
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            db_pool.put(conn)
        except Exception as e:
            logging.exception(f"Error initializing connection pool: {e}")

def get_db_connection():
    """Get a database connection from the pool or create a new one if needed."""
    try:
        # Try to get a connection from the pool with a timeout
        connection = db_pool.get(block=True, timeout=5)
        
        # Test if the connection is valid
        try:
            connection.execute("SELECT 1")
            return connection
        except sqlite3.Error:
            # Connection is stale, create a new one
            try:
                connection.close()
            except:
                pass
            flow_logger.warning("Retrieved stale connection from pool, creating new one")
            return sqlite3.connect(db_file, check_same_thread=False)
            
    except queue.Empty:
        # If pool is empty and timeout expires, create a new connection
        flow_logger.warning("DB pool exhausted, creating new connection")
        return sqlite3.connect(db_file, check_same_thread=False)
    except Exception as e:
        flow_logger.error(f"Error getting DB connection: {e}")
        # Always return a working connection
        return sqlite3.connect(db_file, check_same_thread=False)

def return_db_connection(connection):
    """Return a connection to the pool or close it if pool is full."""
    try:
        # Test if the connection is still valid
        try:
            connection.execute("SELECT 1")
            # Try to return the connection to the pool
            db_pool.put(connection, block=False)
        except (sqlite3.Error, Exception):
            # If connection is no longer valid, close it
            try:
                connection.close()
            except:
                pass
    except queue.Full:
        # If pool is full, close the connection
        try:
            connection.close()
        except:
            pass
    except Exception as e:
        # Ensure connection is closed on any error
        flow_logger.error(f"Error returning DB connection: {e}")
        try:
            connection.close()
        except:
            pass

def get_anonymous_user_id(telegram_id):
    conn = get_db_connection()
    anonymous_id = None
    try:
        cursor = conn.cursor()
        
        # Check if we already have a nickname for this user
        cursor.execute("SELECT nickname FROM user_nicknames WHERE telegram_id = ?", (str(telegram_id),))
        result = cursor.fetchone()
        
        if result:
            # Store existing nickname
            anonymous_id = result[0]
        else:
            # Create a new nickname
            # Hash the telegram_id to ensure consistency
            hash_obj = hashlib.sha256(str(telegram_id).encode())
            hash_hex = hash_obj.hexdigest()
            
            # Use hash to seed the random generator
            random.seed(hash_hex)
            
            # Generate the anonymous ID
            adjective = random.choice(adjectives)
            noun = random.choice(nouns)
            number = random.randint(0, 999)
            anonymous_id = f"{adjective}{noun}{number:03d}"
            
            # Reset the random seed
            random.seed()
            
            # Store the mapping in the database
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                "INSERT INTO user_nicknames (telegram_id, nickname, created_at) VALUES (?, ?, ?)",
                (str(telegram_id), anonymous_id, now)
            )
            conn.commit()
            
    except Exception as e:
        flow_logger.error(f"Error in get_anonymous_user_id: {e}")
        # Make sure to not expose the error details to log if they contain the actual telegram_id
        conn.rollback()
        # Fallback to a generic ID if something goes wrong
        anonymous_id = f"Anonymous{random.randint(10000, 99999)}"
    finally:
        return_db_connection(conn)
        return anonymous_id


def update_user_preferences(anonymous_id, consent=None, age=None, gender=None, 
                           occupation=None, time_in_turku=None, language=None):
    """
    Update or create user preferences in the database
    
    Args:
        anonymous_id (str): Anonymous user ID
        consent (bool, optional): User consent status
        age (str, optional): Age group
        gender (str, optional): Gender
        occupation (str, optional): Occupation
        time_in_turku (str, optional): Time living in Turku
        language (str, optional): Preferred language (en, fi, sv, uk)
        
    Returns:
        bool: Success status
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Check if user already exists
        cursor.execute("SELECT user_id FROM user_preferences WHERE user_id = ?", (anonymous_id,))
        exists = cursor.fetchone()
        
        if exists:
            # Update existing user
            # Build the SET part of the query dynamically based on which fields are provided
            set_parts = []
            params = []
            
            if consent is not None:
                set_parts.append("consent = ?")
                params.append(1 if consent else 0)
            
            if age is not None:
                set_parts.append("age = ?")
                params.append(age)
                
            if gender is not None:
                set_parts.append("gender = ?")
                params.append(gender)
                
            if occupation is not None:
                set_parts.append("occupation = ?")
                params.append(occupation)
                
            if time_in_turku is not None:
                set_parts.append("time_in_turku = ?")
                params.append(time_in_turku)
                
            if language is not None:
                set_parts.append("language = ?")
                params.append(language)
            
            # Always update last_active
            set_parts.append("last_active = ?")
            params.append(now)
            
            # Add user_id to params
            params.append(anonymous_id)
            
            # Execute update
            query = f"UPDATE user_preferences SET {', '.join(set_parts)} WHERE user_id = ?"
            cursor.execute(query, params)
            
        else:
            # Create new user
            cursor.execute(
                """
                INSERT INTO user_preferences 
                (user_id, consent, last_active, age, gender, occupation, time_in_turku, language) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    anonymous_id, 
                    1 if consent else 0 if consent is not None else None, 
                    now, 
                    age if age is not None else 'Not provided', 
                    gender if gender is not None else 'Not provided', 
                    occupation if occupation is not None else 'Not provided', 
                    time_in_turku if time_in_turku is not None else 'Not provided',
                    language if language is not None else 'en'
                )
            )
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        flow_logger.error(f"Error in update_user_preferences: {e}")
        return False
    finally:
        return_db_connection(conn)

def save_submission(anonymous_id, submission_type, standard_selections, custom_inputs,
                  latitude, longitude, venue_title="", venue_address="", additional_info=""):
    """
    Save a new user submission to the database
    
    Args:
        anonymous_id (str): Anonymous user ID
        submission_type (str): 'issue' or 'improvement'
        standard_selections (str): Semicolon-separated list of standard selections
        custom_inputs (str): Semicolon-separated list of custom inputs
        latitude (float): Location latitude
        longitude (float): Location longitude
        venue_title (str, optional): Venue title
        venue_address (str, optional): Venue address
        additional_info (str, optional): Additional information
        
    Returns:
        int: Submission ID or 0 if failed
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute(
            """
            INSERT INTO submissions
            (user_id, submission_type, standard_selections, custom_inputs, 
             latitude, longitude, venue_title, venue_address, additional_info, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                anonymous_id,
                submission_type,
                standard_selections,
                custom_inputs,
                str(latitude),
                str(longitude),
                venue_title,
                venue_address,
                additional_info,
                now
            )
        )
        
        conn.commit()
        submission_id = cursor.lastrowid
        
        flow_logger.info(f"Saved submission {submission_id} for user: {anonymous_id}")
        return submission_id
    except Exception as e:
        conn.rollback()
        flow_logger.error(f"Error in save_submission: {e}")
        return 0
    finally:
        return_db_connection(conn)

def export_data_to_csv():
    """
    Export all data from the database to CSV
    
    Returns:
        str: Path to the exported CSV file
    """
    conn = get_db_connection()
    try:
        import csv
        
        # Output file path
        output_dir = '/scratch/project_2004147/telebot'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        output_file = os.path.join(output_dir, f'city_issue_data_export_{int(time.time())}.csv')
        
        # Get all submissions with user info
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT s.id, s.user_id, s.submission_type, s.standard_selections, s.custom_inputs,
                   s.latitude, s.longitude, s.venue_title, s.venue_address, s.additional_info,
                   p.age, p.gender, p.occupation, p.time_in_turku, s.timestamp
            FROM submissions s
            LEFT JOIN user_preferences p ON s.user_id = p.user_id
            ORDER BY s.id
            """
        )
        
        rows = cursor.fetchall()
        
        # Column names for the CSV
        fieldnames = [
            'id', 'anonymous_id', 'submission_type', 'standard_selections', 'custom_inputs',
            'latitude', 'longitude', 'venue_title', 'venue_address', 'additional_info',
            'age', 'gender', 'occupation', 'time_in_turku', 'timestamp'
        ]
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL)
            
            # Write header
            writer.writerow(fieldnames)
            
            # Write data rows
            for row in rows:
                writer.writerow(row)
        
        return output_file
    except Exception as e:
        flow_logger.error(f"Error in export_data_to_csv: {e}")
        return None
    finally:
        return_db_connection(conn)


def get_user_language(user_id):
    """
    Get the user's preferred language from database
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        str: Language code ('en', 'fi', 'sv', 'uk') with default 'en'
    """
    try:
        # First check if it's in user_data (for current session)
        if user_id in user_data and 'language' in user_data[user_id]:
            # Validate language code is supported
            lang_code = user_data[user_id]['language']
            if lang_code in messages:
                return lang_code
        
        # Otherwise check in the database
        anonymous_id = get_anonymous_user_id(user_id)
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT language FROM user_preferences WHERE user_id = ?", (anonymous_id,))
        result = cursor.fetchone()
        
        return_db_connection(conn)
        
        if result and result[0] and result[0] in messages:
            # Store in user_data for current session
            if user_id not in user_data:
                user_data[user_id] = {}
            user_data[user_id]['language'] = result[0]
            return result[0]
        else:
            return 'en'  # Default to English
    except Exception as e:
        flow_logger.error(f"Error retrieving user language: {e}")
        return 'en'  # Default to English in case of error
def update_welcome_message():
    for lang in messages:
        if lang in menu_messages:
            # Add menu options to each language in the messages dictionary
            for key, value in menu_messages[lang].items():
                messages[lang][key] = value

# Next, let's add a new function to send the main menu
def send_main_menu(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Create inline keyboard with menu options
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        
        # Add report issue/improvement button
        report_button = types.InlineKeyboardButton(
            text=messages[lang_code]['menu_report'],
            callback_data="menu_report"
        )
        
        # Add privacy notice button
        privacy_button = types.InlineKeyboardButton(
            text=messages[lang_code]['menu_privacy'],
            callback_data="menu_privacy"
        )
        
        # Add participant information button
        info_button = types.InlineKeyboardButton(
            text=messages[lang_code]['menu_info'],
            callback_data="menu_info"
        )
        
        # Add language selection button
        language_button = types.InlineKeyboardButton(
            text=messages[lang_code]['menu_language'],
            callback_data="menu_language"
        )
        
        # Add all buttons to keyboard
        inline_kb.add(report_button, privacy_button, info_button, language_button)
        
        # Send welcome message with menu
        bot.send_message(
            chat_id,
            messages[lang_code]['welcome'] + "\n\n" + messages[lang_code]['menu_options'],
            reply_markup=inline_kb
        )
        
    except Exception as e:
        logging.exception(f"Error in send_main_menu: {e}")
        try:
            lang_code = get_user_language(user_id)
            bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        except:
            bot.send_message(chat_id, "An error occurred. Please try again later.")

@bot.message_handler(commands=['start'])
def send_welcome(message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Log that we received a start command

        # Initialize user data
        if user_id not in user_data:
            user_data[user_id] = {}

        # Check if user already has a language preference
        anonymous_id = get_anonymous_user_id(user_id)
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT language FROM user_preferences WHERE user_id = ?", (anonymous_id,))
        result = cursor.fetchone()
        
        return_db_connection(conn)
        
        if result and result[0] and result[0] in messages:
            # User already has a valid language preference
            lang_code = result[0]
            user_data[user_id]['language'] = lang_code
            
            
            # Send main menu in user's preferred language
            send_main_menu(chat_id, user_id)
        else:
            # No valid language preference, ask user to select language first
            ask_language_selection(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in send_welcome: {e}")
        # Use error message in user's language if available
        lang_code = user_data.get(user_id, {}).get('language', 'en') if 'user_id' in locals() else 'en'
        bot.send_message(message.chat.id, messages[lang_code]['error_occurred'])

@bot.callback_query_handler(func=lambda call: call.data.startswith('menu_'))
def handle_menu_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        option = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        if option == 'report':
            # Check for consent first
            if user_id in user_data and user_data[user_id].get('consent') == True:
                # User has already given consent, proceed to location request
                ask_location(chat_id, user_id)
            else:
                # Ask for consent first
                ask_for_consent(chat_id, user_id)
        
        elif option == 'privacy':
            # Send privacy notice
            inline_kb = types.InlineKeyboardMarkup(row_width=1)
            link_button = types.InlineKeyboardButton(
                text=messages[lang_code]['privacy_notice_title'], 
                url=privacy_links[lang_code]['privacy_notice']
            )
            back_button = types.InlineKeyboardButton(
                text=messages[lang_code]['back_to_menu'], 
                callback_data="back_to_menu"
            )
            inline_kb.add(link_button, back_button)
            
            bot.send_message(
                chat_id,
                messages[lang_code]['privacy_notice_link'],
                reply_markup=inline_kb
            )
        
        elif option == 'info':
            # Send participant information
            inline_kb = types.InlineKeyboardMarkup(row_width=1)
            link_button = types.InlineKeyboardButton(
                text=messages[lang_code]['participant_info_title'], 
                url=privacy_links[lang_code]['participant_info']
            )
            back_button = types.InlineKeyboardButton(
                text=messages[lang_code]['back_to_menu'], 
                callback_data="back_to_menu"
            )
            inline_kb.add(link_button, back_button)
            
            bot.send_message(
                chat_id,
                messages[lang_code]['participant_info_link'],
                reply_markup=inline_kb
            )
        
        elif option == 'language':
            # Ask for language selection again
            ask_language_selection(chat_id, user_id)
    
    except Exception as e:
        logging.exception(f"Error in handle_menu_selection: {e}")
        try:
            lang_code = get_user_language(user_id)
            bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        except:
            bot.send_message(chat_id, "An error occurred. Please try again later.")

@bot.callback_query_handler(func=lambda call: call.data == "back_to_menu")
def handle_back_to_menu(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        # Acknowledge the action
        bot.answer_callback_query(call.id, "Returning to main menu...")
        
        # Send main menu
        send_main_menu(chat_id, user_id)
    
    except Exception as e:
        logging.exception(f"Error in handle_back_to_menu: {e}")
        try:
            lang_code = get_user_language(user_id)
            bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        except:
            bot.send_message(chat_id, "An error occurred. Please try again later.")



# Fix 2: Ensure the ask_language_selection function is working correctly

def ask_language_selection(chat_id, user_id):
    """
    Ask user to select their preferred language
    
    Args:
        chat_id (int): Telegram chat ID
        user_id (int): Telegram user ID
    """
    try:
        
        # Create keyboard with language options
        inline_kb = types.InlineKeyboardMarkup(row_width=2)
        
        # Language options with flags
        lang_buttons = [
            types.InlineKeyboardButton(text="English 🇬🇧", callback_data="lang_en"),
            types.InlineKeyboardButton(text="Suomi 🇫🇮", callback_data="lang_fi"),
            types.InlineKeyboardButton(text="Svenska 🇸🇪", callback_data="lang_sv"),
            types.InlineKeyboardButton(text="Українська 🇺🇦", callback_data="lang_uk")
        ]
        
        inline_kb.add(*lang_buttons)
        
        # Send language selection message in all supported languages
        message = bot.send_message(
            chat_id,
            "Please select your language / Valitse kieli / Välj språk / Оберіть мову:",
            reply_markup=inline_kb
        )
        
    except Exception as e:
        logging.exception(f"Error in ask_language_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")



@bot.callback_query_handler(func=lambda call: call.data.startswith('lang_'))
def handle_language_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Extract language code
        lang_code = call.data.split('_')[1]
        
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        # Get anonymous ID
        anonymous_id = get_anonymous_user_id(user_id)
        
        # Update language preference in database
        update_user_preferences(anonymous_id=anonymous_id, language=lang_code)
        
        # Initialize user data if needed
        if user_id not in user_data:
            user_data[user_id] = {}
        
        # Store language in user_data for current session
        user_data[user_id]['language'] = lang_code
        
        # Acknowledge language selection
        bot.answer_callback_query(call.id, messages[lang_code]['language_selected'])
        
        # Send language selected confirmation
        bot.send_message(
            chat_id,
            messages[lang_code]['language_selected']
        )
        
        # Send main menu in the selected language
        send_main_menu(chat_id, user_id)
        
    except Exception as e:
        logging.exception(f"Error in handle_language_selection: {e}")
        bot.send_message(call.message.chat.id, "An error occurred. Please try again later.")


     
# Update the consent function to use language-specific messages
def ask_for_consent(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        inline_kb = types.InlineKeyboardMarkup(row_width=2)
        buttons = [
            types.InlineKeyboardButton(
                text=option, 
                callback_data=f"consent_{i}"
            )
            for i, option in enumerate(messages[lang_code]['consent_options'])
        ]
        inline_kb.add(*buttons)
        
        bot.send_message(
            chat_id,
            messages[lang_code]['consent_prompt'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_for_consent: {e}")
        # Get language code for error message
        try:
            lang_code = get_user_language(user_id)
            bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        except:
            # Fallback to English if language retrieval fails
            bot.send_message(chat_id, "An error occurred. Please try again later.")


# Update the consent handler
@bot.callback_query_handler(func=lambda call: call.data.startswith('consent_'))
def handle_consent(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        choice = int(call.data.split('_')[1])
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Get anonymous ID for database update
        anonymous_id = get_anonymous_user_id(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        if choice == 0:  # I agree
            # Store consent in user data
            user_data[user_id]['consent'] = True
            
            # Update user preferences in the database
            update_user_preferences(anonymous_id=anonymous_id, consent=True)
            
            # Acknowledge consent
            bot.answer_callback_query(call.id, messages[lang_code]['consent_given'])
            
            # Send consent confirmation message
            bot.send_message(
                chat_id,
                messages[lang_code]['consent_given']
            )
            
            # Proceed to location request
            ask_location(chat_id, user_id)
        else:  # I do not agree
            # Store lack of consent
            user_data[user_id]['consent'] = False
            
            # Update user preferences in the database
            update_user_preferences(anonymous_id=anonymous_id, consent=False)
            
            # Acknowledge decision
            bot.answer_callback_query(call.id, "You've declined to share your information.")
            
            # Provide restart option
            inline_kb = types.InlineKeyboardMarkup(row_width=1)
            restart_button = types.InlineKeyboardButton(
                text=messages[lang_code]['restart_button'], 
                callback_data="restart_bot"
            )
            inline_kb.add(restart_button)
            
            bot.send_message(
                chat_id,
                messages[lang_code]['consent_declined'],
                reply_markup=inline_kb
            )
    except Exception as e:
        logging.exception(f"Error in handle_consent: {e}")
        # Try to get user's language for error message
        try:
            lang_code = get_user_language(user_id)
            bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        except:
            # Fallback to English
            bot.send_message(chat_id, "An error occurred. Please try again later.")


@bot.callback_query_handler(func=lambda call: call.data == "restart_bot")
def handle_restart(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        # Acknowledge restart
        bot.answer_callback_query(call.id, "Restarting the bot...")
        
        # Get language before clearing user data
        saved_language = user_data[user_id].get('language', 'en')
        
        # Clear user data but retain language preference
        user_data[user_id] = {'language': saved_language}
        
        # Start over with welcome message
        bot.send_message(
            chat_id,
            messages[saved_language]['welcome']
        )
        
        # Continue with consent request
        ask_for_consent(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in handle_restart: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('action_'))
def handle_action_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if not user_data[user_id].get('action_types', []):
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
                
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Get selected actions
            selected_actions = user_data[user_id]['action_types']
            actions_str = ', '.join(selected_actions)
            
            bot.send_message(
                chat_id,
                f"{messages[lang_code]['your_response']} {actions_str}"
            )
            
            # Clear awaiting_multiple_select
            user_data[user_id].pop('awaiting_multiple_select', None)
            
            # Process selected actions based on language-specific options
            action_options = messages[lang_code]['action_options']
            
            if action_options[0] in selected_actions and action_options[1] in selected_actions:
                # Both issue and improvement selected
                user_data[user_id]['action_type'] = 'both'
                
                # If in modify mode, return to summary after completing issue/improvement selections
                if user_data[user_id].get('is_modifying'):
                    user_data[user_id]['return_to_summary_after_both'] = True
                
                ask_issue_list(chat_id, user_id)
            elif action_options[0] in selected_actions:
                # Only issue selected
                user_data[user_id]['action_type'] = 'issue'
                ask_issue_list(chat_id, user_id)
            elif action_options[1] in selected_actions:
                # Only improvement selected
                user_data[user_id]['action_type'] = 'improvement'
                ask_improvement_list(chat_id, user_id)
        else:
            idx = int(data)
            options = messages[lang_code]['action_options']
            
            if 0 <= idx < len(options):
                choice = options[idx]
                
                # Initialize action_types if not already
                if 'action_types' not in user_data[user_id]:
                    user_data[user_id]['action_types'] = []
                
                # Toggle selection
                if choice in user_data[user_id]['action_types']:
                    user_data[user_id]['action_types'].remove(choice)
                    bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {choice}")
                else:
                    user_data[user_id]['action_types'].append(choice)
                    bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {choice}")
                
                update_action_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_action_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

def update_action_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['action_options']
        selected_options = user_data[user_id]['action_types']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if option in selected_options else option
            callback_data = f"action_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="action_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_action_keyboard: {e}")






def ask_issue_list(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Clear old values if any
        user_data[user_id]['issue_type'] = []
        user_data[user_id]['custom_issue'] = []
        user_data[user_id]['awaiting_multiple_select'] = 'issue'
        
        options = messages[lang_code]['issue_list']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"issue_{idx}")
            for idx, option in enumerate(options)
        ]
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="issue_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        instruction_text = messages[lang_code]['issue_list_prompt']
        
        bot.send_message(
            chat_id,
            instruction_text,
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_issue_list: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the improvement list function to use language-specific options
def ask_improvement_list(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Clear old values if any
        user_data[user_id]['improvement_type'] = []
        user_data[user_id]['custom_improvement'] = []
        user_data[user_id]['awaiting_multiple_select'] = 'improvement'
        
        options = messages[lang_code]['improvement_list']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"improvement_{idx}")
            for idx, option in enumerate(options)
        ]
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="improvement_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        instruction_text = messages[lang_code]['improvement_list_prompt']
        
        bot.send_message(
            chat_id,
            instruction_text,
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_improvement_list: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


  
@bot.callback_query_handler(func=lambda call: call.data.startswith('issue_'))
def handle_issue_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if not user_data[user_id]['issue_type'] and not user_data[user_id].get('custom_issue', []):
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
                
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Combine selected options and custom inputs
            all_issues = user_data[user_id]['issue_type'] + user_data[user_id].get('custom_issue', [])
            issues_str = ', '.join(all_issues)
            
            bot.send_message(
                chat_id,
                f"{messages[lang_code]['your_response']}: {issues_str}"
            )
            
            # Clear awaiting_multiple_select
            user_data[user_id].pop('awaiting_multiple_select', None)
            
            # Check if modifying and both actions were selected
            if user_data[user_id].get('is_modifying') and user_data[user_id]['action_type'] != 'both':
                # Return to summary if just modifying issues
                user_data[user_id].pop('is_modifying', None)
                ask_final_confirmation(chat_id, user_id)
            elif user_data[user_id]['action_type'] == 'both':
                # If both, proceed to improvement list
                ask_improvement_list(chat_id, user_id)
            else:
                # If just issues and not modifying, proceed to additional info
                ask_additional_info(chat_id, user_id)
        else:
            idx = int(data)
            options = messages[lang_code]['issue_list']
            
            if 0 <= idx < len(options):
                choice = options[idx]
                
                # Check if "Other" option was selected
                if choice == messages[lang_code]['other_option']:
                    # Prompt for custom input
                    bot.answer_callback_query(call.id, messages[lang_code]['specify_other'])
                    bot.send_message(chat_id, messages[lang_code]['specify_other'])
                    # Next message will be caught by handle_text_input
                    return
                
                # Toggle selection
                if choice in user_data[user_id]['issue_type']:
                    user_data[user_id]['issue_type'].remove(choice)
                    bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']}: {choice}")
                else:
                    user_data[user_id]['issue_type'].append(choice)
                    bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']}: {choice}")
                
                update_issue_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_issue_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

def update_issue_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['issue_list']
        selected_options = user_data[user_id]['issue_type']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if option in selected_options else option
            callback_data = f"issue_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="issue_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_issue_keyboard: {e}")


@bot.callback_query_handler(func=lambda call: call.data.startswith('improvement_'))
def handle_improvement_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if not user_data[user_id]['improvement_type'] and not user_data[user_id].get('custom_improvement', []):
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
                
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Combine selected options and custom inputs
            all_improvements = user_data[user_id]['improvement_type'] + user_data[user_id].get('custom_improvement', [])
            improvements_str = ', '.join(all_improvements)
            
            bot.send_message(
                chat_id,
                f"{messages[lang_code]['your_response']} {improvements_str}"
            )
            
            # Clear awaiting_multiple_select
            user_data[user_id].pop('awaiting_multiple_select', None)
            
            # Check if this is the second part of "both" in modify mode
            if user_data[user_id].get('return_to_summary_after_both'):
                user_data[user_id].pop('return_to_summary_after_both', None)
                user_data[user_id].pop('is_modifying', None)
                ask_final_confirmation(chat_id, user_id)
            # Check if modifying
            elif user_data[user_id].get('is_modifying'):
                user_data[user_id].pop('is_modifying', None)
                ask_final_confirmation(chat_id, user_id)
            else:
                # Regular flow - proceed to additional info
                ask_additional_info(chat_id, user_id)
        else:
            idx = int(data)
            options = messages[lang_code]['improvement_list']
            
            if 0 <= idx < len(options):
                choice = options[idx]
                
                # Check if "Other" option was selected
                if choice == messages[lang_code]['other_option']:
                    # Prompt for custom input
                    bot.answer_callback_query(call.id, messages[lang_code]['specify_other'])
                    bot.send_message(chat_id, messages[lang_code]['specify_other'])
                    # Next message will be caught by handle_text_input
                    return
                
                # Toggle selection
                if choice in user_data[user_id]['improvement_type']:
                    user_data[user_id]['improvement_type'].remove(choice)
                    bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {choice}")
                else:
                    user_data[user_id]['improvement_type'].append(choice)
                    bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {choice}")
                
                update_improvement_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_improvement_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


# Update the improvement keyboard function
def update_improvement_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['improvement_list']
        selected_options = user_data[user_id]['improvement_type']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if option in selected_options else option
            callback_data = f"improvement_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="improvement_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_improvement_keyboard: {e}")



# Update the location request function
def ask_location(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Send location request message
        bot.send_message(
            chat_id,
            messages[lang_code]['location_request']
        )
        
        # Register the next step handler
        bot.register_next_step_handler_by_chat_id(chat_id, handle_location)
    except Exception as e:
        logging.exception(f"Error in ask_location: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the location handler
def handle_location(message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if message.content_type == 'location':
            # Extract location data
            latitude = message.location.latitude
            longitude = message.location.longitude
            
            # Store location data
            user_data[user_id]['location'] = {
                'latitude': latitude,
                'longitude': longitude,
                'venue_title': '',
                'venue_address': ''
            }
            
            # Confirm location received
            bot.send_message(
                chat_id,
                f"📍 {messages[lang_code]['location_received']}"
            )
            
            # If in modify mode, return to summary
            if user_data[user_id].get('is_modifying'):
                user_data[user_id].pop('is_modifying', None)
                ask_final_confirmation(chat_id, user_id)
            else:
                # Otherwise proceed to action selection
                ask_action_selection(chat_id, user_id)
            
        elif message.content_type == 'venue':
            # Extract venue data
            latitude = message.venue.location.latitude
            longitude = message.venue.location.longitude
            venue_title = message.venue.title
            venue_address = message.venue.address if message.venue.address else ''
            
            # Store venue data
            user_data[user_id]['location'] = {
                'latitude': latitude,
                'longitude': longitude,
                'venue_title': venue_title,
                'venue_address': venue_address
            }
            
            # Confirm venue received
            bot.send_message(
                chat_id,
                f"📍 {messages[lang_code]['location_received']}: {venue_title}"
            )
            
            # If in modify mode, return to summary
            if user_data[user_id].get('is_modifying'):
                user_data[user_id].pop('is_modifying', None)
                ask_final_confirmation(chat_id, user_id)
            else:
                # Otherwise proceed to action selection
                ask_action_selection(chat_id, user_id)
            
        else:
            # Not a location or venue message
            bot.send_message(
                chat_id,
                messages[lang_code]['please_send_location']
            )
            
            # Ask again for location
            bot.register_next_step_handler_by_chat_id(chat_id, handle_location)
    except Exception as e:
        logging.exception(f"Error in handle_location: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")



def ask_action_selection(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Clear old values if any
        user_data[user_id]['action_types'] = []
        user_data[user_id]['awaiting_multiple_select'] = 'action'
        
        options = messages[lang_code]['action_options']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"action_{idx}")
            for idx, option in enumerate(options)
        ]
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],  
            callback_data="action_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        instruction_text = f"{messages[lang_code]['select_action']}"
        
        bot.send_message(
            chat_id,
            instruction_text,
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_action_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


def ask_additional_info(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Create inline keyboard with Skip button
        inline_kb = types.InlineKeyboardMarkup()
        skip_button = types.InlineKeyboardButton(
            text=messages[lang_code]['skip_button'], 
            callback_data='skip_additional_info'
        )
        inline_kb.add(skip_button)
        
        # Send message asking for additional info
        bot.send_message(
            chat_id,
            messages[lang_code]['additional_info_prompt'],
            reply_markup=inline_kb
        )
        
        # Register next step handler
        bot.register_next_step_handler_by_chat_id(chat_id, handle_additional_info)
    except Exception as e:
        logging.exception(f"Error in ask_additional_info: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the skip additional info handler
@bot.callback_query_handler(func=lambda call: call.data == 'skip_additional_info')
def handle_skip_additional_info(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        # Acknowledge the skip action
        bot.answer_callback_query(call.id, messages[lang_code]['skip_button'])
        
        # Clear the next step handler
        bot.clear_step_handler_by_chat_id(chat_id)
        
        # Store empty additional info
        user_data[user_id]['additional_info'] = ''
        
        # Check if we're returning from modify flow
        if user_data[user_id].get('returning_from_modify'):
            # Remove the flag
            user_data[user_id].pop('returning_from_modify', None)
            # Return to summary
            ask_final_confirmation(chat_id, user_id)
        else:
            # Proceed to socioeconomic info
            ask_socioeconomic_info(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in handle_skip_additional_info: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

def handle_additional_info(message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if message.content_type == 'text':
            # Store the additional info
            user_data[user_id]['additional_info'] = message.text.strip()
            
            # Check if we're returning from modify flow
            if user_data[user_id].get('returning_from_modify'):
                # Remove the flag
                user_data[user_id].pop('returning_from_modify', None)
                # Return to summary
                ask_final_confirmation(chat_id, user_id)
            else:
                # Proceed to socioeconomic info
                ask_socioeconomic_info(chat_id, user_id)
        else:
            # Not a text message
            bot.send_message(
                chat_id,
                messages[lang_code]['please_send_location']
            )
            
            # Ask again for additional info
            ask_additional_info(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in handle_additional_info: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

def ask_socioeconomic_info(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Get the anonymous ID for the user
        anonymous_id = get_anonymous_user_id(user_id)
        
        # Check if user has already provided socioeconomic data
        existing_data = check_user_socioeconomic_data(anonymous_id)
        
        # If we're modifying, always show the consent question
        if user_data[user_id].get('is_modifying') or not existing_data:
            # No existing data or modifying, ask user if they want to provide socioeconomic info
            inline_kb = types.InlineKeyboardMarkup(row_width=2)
            yes_button = types.InlineKeyboardButton(
                text=messages[lang_code]['socioeconomic_options'][0], 
                callback_data="socio_yes"
            )
            no_button = types.InlineKeyboardButton(
                text=messages[lang_code]['socioeconomic_options'][1], 
                callback_data="socio_no"
            )
            inline_kb.add(yes_button, no_button)
            
            # Send message asking if user wants to share socioeconomic info
            bot.send_message(
                chat_id,
                messages[lang_code]['socioeconomic_intro'],
                reply_markup=inline_kb
            )
        else:
            # User has already provided socioeconomic data and we're not modifying
            flow_logger.info(f"Using existing socioeconomic data for user: {anonymous_id}")
            
            # Store the existing socioeconomic data in user_data
            user_data[user_id]['age'] = existing_data['age']
            user_data[user_id]['gender'] = existing_data['gender']
            user_data[user_id]['occupation'] = existing_data['occupation']
            user_data[user_id]['time_in_turku'] = existing_data['time_in_turku']
            
            # Send a message to inform user
            bot.send_message(
                chat_id,
                "Using your previously provided personal information. "
                "You can proceed to review your submission."
            )
            
            # Skip to final confirmation
            ask_final_confirmation(chat_id, user_id)
        
    except Exception as e:
        logging.exception(f"Error in ask_socioeconomic_info: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the socioeconomic choice handler
@bot.callback_query_handler(func=lambda call: call.data.startswith('socio_'))
def handle_socioeconomic_choice(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        choice = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        if choice == 'yes':  # Yes, I'll share
            # Acknowledge the choice
            bot.answer_callback_query(call.id, messages[lang_code]['socioeconomic_options'][0])
            
            # Start with age question
            ask_age(chat_id, user_id)
        else:  # No, skip this part
            # Acknowledge the choice
            bot.answer_callback_query(call.id, messages[lang_code]['socioeconomic_options'][1])
            
            # Initialize empty socioeconomic data fields
            user_data[user_id]['age'] = 'Not provided'
            user_data[user_id]['gender'] = 'Not provided'
            user_data[user_id]['occupation'] = 'Not provided'
            user_data[user_id]['time_in_turku'] = 'Not provided'
            
            # Store the user's preference to not share socioeconomic data
            anonymous_id = get_anonymous_user_id(user_id)
            update_user_preferences(
                anonymous_id=anonymous_id,
                age='Not provided',
                gender='Not provided',
                occupation='Not provided',
                time_in_turku='Not provided'
            )
            
            # Check if we're in modify mode
            if user_data[user_id].get('is_modifying'):
                # Remove the flag
                user_data[user_id].pop('is_modifying', None)
                # Return to summary
                ask_final_confirmation(chat_id, user_id)
            else:
                # Regular flow - proceed to final confirmation
                ask_final_confirmation(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in handle_socioeconomic_choice: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the age question function
def ask_age(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Initialize/reset age selection
        user_data[user_id]['age_selected'] = None
        
        # Create inline keyboard for age options
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"age_{idx}")
            for idx, option in enumerate(messages[lang_code]['age_options'])
        ]
        # Add Done button (disabled initially since no selection has been made)
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="age_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        # Send message asking for age
        bot.send_message(
            chat_id,
            messages[lang_code]['age_question'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_age: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update age selection handler
@bot.callback_query_handler(func=lambda call: call.data.startswith('age_'))
def handle_age_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if user_data[user_id].get('age_selected') is None:
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
            
            # Selection confirmed, store the selected age
            user_data[user_id]['age'] = messages[lang_code]['age_options'][user_data[user_id]['age_selected']]
            
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Acknowledge the confirmation
            bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {user_data[user_id]['age']}")
            
            # Check if we're returning from modify flow
            if user_data[user_id].get('returning_from_modify'):
                # If only modifying age, update the database and return to summary
                if user_data[user_id].get('gender') and user_data[user_id].get('occupation') and user_data[user_id].get('time_in_turku'):
                    # Update database
                    anonymous_id = get_anonymous_user_id(user_id)
                    update_user_preferences(
                        anonymous_id=anonymous_id,
                        age=user_data[user_id]['age']
                    )
                    # Return to summary
                    ask_final_confirmation(chat_id, user_id)
                else:
                    # Continue with gender question
                    ask_gender(chat_id, user_id)
            else:
                # Proceed to gender question
                ask_gender(chat_id, user_id)
        else:
            idx = int(data)
            if 0 <= idx < len(messages[lang_code]['age_options']):
                # Store the temporarily selected age
                user_data[user_id]['age_selected'] = idx
                
                # Acknowledge the selection
                bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {messages[lang_code]['age_options'][idx]}")
                
                # Update the keyboard to show selection
                update_age_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_age_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

def check_user_socioeconomic_data(anonymous_id):
    """
    Check if a user has already provided socioeconomic data
    
    Args:
        anonymous_id (str): Anonymous user ID
        
    Returns:
        dict: Dictionary containing socioeconomic data if available, otherwise None
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if user already exists in preferences with socioeconomic data
        cursor.execute(
            """
            SELECT age, gender, occupation, time_in_turku 
            FROM user_preferences 
            WHERE user_id = ? AND 
                  age IS NOT NULL AND age != 'Not provided' AND
                  gender IS NOT NULL AND gender != 'Not provided' AND
                  occupation IS NOT NULL AND occupation != 'Not provided' AND
                  time_in_turku IS NOT NULL AND time_in_turku != 'Not provided'
            """, 
            (anonymous_id,)
        )
        result = cursor.fetchone()
        
        if result:
            # User has socioeconomic data
            return {
                'age': result[0],
                'gender': result[1],
                'occupation': result[2],
                'time_in_turku': result[3]
            }
        else:
            # No socioeconomic data available
            return None
    except Exception as e:
        flow_logger.error(f"Error in check_user_socioeconomic_data: {e}")
        return None
    finally:
        return_db_connection(conn)


# Update the age keyboard function
def update_age_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['age_options']
        selected_idx = user_data[user_id]['age_selected']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if idx == selected_idx else option
            callback_data = f"age_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="age_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_age_keyboard: {e}")

# Update the gender question function
def ask_gender(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Initialize/reset gender selection
        user_data[user_id]['gender_selected'] = None
        
        # Create inline keyboard for gender options
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"gender_{idx}")
            for idx, option in enumerate(messages[lang_code]['gender_options'])
        ]
        # Add Done button
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'], 
            callback_data="gender_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        # Send message asking for gender
        bot.send_message(
            chat_id,
            messages[lang_code]['gender_question'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_gender: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the gender selection handler
@bot.callback_query_handler(func=lambda call: call.data.startswith('gender_'))
def handle_gender_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if user_data[user_id].get('gender_selected') is None:
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
            
            # Selection confirmed, store the selected gender
            user_data[user_id]['gender'] = messages[lang_code]['gender_options'][user_data[user_id]['gender_selected']]
            
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Acknowledge the confirmation
            bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {user_data[user_id]['gender']}")
            
            # Check if we're returning from modify flow
            if user_data[user_id].get('returning_from_modify'):
                # If only modifying gender, update the database and return to summary
                if user_data[user_id].get('age') and user_data[user_id].get('occupation') and user_data[user_id].get('time_in_turku'):
                    # Update database
                    anonymous_id = get_anonymous_user_id(user_id)
                    update_user_preferences(
                        anonymous_id=anonymous_id,
                        gender=user_data[user_id]['gender']
                    )
                    # Return to summary
                    ask_final_confirmation(chat_id, user_id)
                else:
                    # Continue with occupation question
                    ask_occupation(chat_id, user_id)
            else:
                # Proceed to occupation question
                ask_occupation(chat_id, user_id)
        else:
            idx = int(data)
            if 0 <= idx < len(messages[lang_code]['gender_options']):
                # Store the temporarily selected gender
                user_data[user_id]['gender_selected'] = idx
                
                # Acknowledge the selection
                bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {messages[lang_code]['gender_options'][idx]}")
                
                # Update the keyboard to show selection
                update_gender_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_gender_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the gender keyboard function
def update_gender_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['gender_options']
        selected_idx = user_data[user_id]['gender_selected']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if idx == selected_idx else option
            callback_data = f"gender_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="gender_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_gender_keyboard: {e}")

# Update the occupation question function
def ask_occupation(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Initialize/reset occupation selection
        user_data[user_id]['occupation_selected'] = None
        
        # Create inline keyboard for occupation options
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"occupation_{idx}")
            for idx, option in enumerate(messages[lang_code]['occupation_options'])
        ]
        # Add Done button
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="occupation_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        # Send message asking for occupation
        bot.send_message(
            chat_id,
            messages[lang_code]['occupation_question'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_occupation: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('occupation_'))
def handle_occupation_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if user_data[user_id].get('occupation_selected') is None:
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
            
            # Selection confirmed, store the selected occupation
            user_data[user_id]['occupation'] = messages[lang_code]['occupation_options'][user_data[user_id]['occupation_selected']]
            
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Acknowledge the confirmation
            bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {user_data[user_id]['occupation']}")
            
            # Check if we're returning from modify flow
            if user_data[user_id].get('returning_from_modify'):
                # If only modifying occupation, update the database and return to summary
                if user_data[user_id].get('age') and user_data[user_id].get('gender') and user_data[user_id].get('time_in_turku'):
                    # Update database
                    anonymous_id = get_anonymous_user_id(user_id)
                    update_user_preferences(
                        anonymous_id=anonymous_id,
                        occupation=user_data[user_id]['occupation']
                    )
                    # Return to summary
                    ask_final_confirmation(chat_id, user_id)
                else:
                    # Continue with time_in_turku question
                    ask_time_in_turku(chat_id, user_id)
            else:
                # Proceed to time_in_turku question
                ask_time_in_turku(chat_id, user_id)
        else:
            idx = int(data)
            if 0 <= idx < len(messages[lang_code]['occupation_options']):
                # Store the temporarily selected occupation
                user_data[user_id]['occupation_selected'] = idx
                
                # Acknowledge the selection
                bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {messages[lang_code]['occupation_options'][idx]}")
                
                # Update the keyboard to show selection
                update_occupation_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_occupation_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the occupation keyboard function
def update_occupation_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['occupation_options']
        selected_idx = user_data[user_id]['occupation_selected']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if idx == selected_idx else option
            callback_data = f"occupation_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="occupation_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_occupation_keyboard: {e}")

# Update the time in Turku question function
def ask_time_in_turku(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Initialize/reset time in Turku selection
        user_data[user_id]['time_in_turku_selected'] = None
        
        # Create inline keyboard for time in Turku options
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = [
            types.InlineKeyboardButton(text=option, callback_data=f"time_{idx}")
            for idx, option in enumerate(messages[lang_code]['time_in_turku_options'])
        ]
        # Add Done button
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="time_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        # Send message asking for time in Turku
        bot.send_message(
            chat_id,
            messages[lang_code]['time_in_turku_question'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_time_in_turku: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('time_'))
def handle_time_in_turku_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        data = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if data == 'done':
            if user_data[user_id].get('time_in_turku_selected') is None:
                bot.answer_callback_query(call.id, messages[lang_code]['please_select_at_least_one'])
                return
            
            # Selection confirmed, store the selected time in Turku
            user_data[user_id]['time_in_turku'] = messages[lang_code]['time_in_turku_options'][user_data[user_id]['time_in_turku_selected']]
            
            # Remove the inline keyboard
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
            
            # Acknowledge the confirmation
            bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {user_data[user_id]['time_in_turku']}")
            
            # Store the socioeconomic data immediately
            anonymous_id = get_anonymous_user_id(user_id)
            update_user_preferences(
                anonymous_id=anonymous_id,
                age=user_data[user_id]['age'],
                gender=user_data[user_id]['gender'],
                occupation=user_data[user_id]['occupation'],
                time_in_turku=user_data[user_id]['time_in_turku'],
                language=lang_code
            )
            
            # Check if we're in modify mode
            if user_data[user_id].get('is_modifying'):
                # Remove the flag
                user_data[user_id].pop('is_modifying', None)
                # Return to summary
                ask_final_confirmation(chat_id, user_id)
            else:
                # Regular flow - proceed to final confirmation
                ask_final_confirmation(chat_id, user_id)
        else:
            idx = int(data)
            if 0 <= idx < len(messages[lang_code]['time_in_turku_options']):
                # Store the temporarily selected time in Turku
                user_data[user_id]['time_in_turku_selected'] = idx
                
                # Acknowledge the selection
                bot.answer_callback_query(call.id, f"{messages[lang_code]['your_response']} {messages[lang_code]['time_in_turku_options'][idx]}")
                
                # Update the keyboard to show selection
                update_time_in_turku_keyboard(call.message, user_id)
            else:
                bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
    except Exception as e:
        logging.exception(f"Error in handle_time_in_turku_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


# Update the time in Turku keyboard function
def update_time_in_turku_keyboard(message, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        options = messages[lang_code]['time_in_turku_options']
        selected_idx = user_data[user_id]['time_in_turku_selected']
        
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        
        for idx, option in enumerate(options):
            button_text = f"✔️ {option}" if idx == selected_idx else option
            callback_data = f"time_{idx}"
            buttons.append(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="time_done"
        )
        inline_kb.add(*buttons)
        inline_kb.add(done_button)
        
        bot.edit_message_reply_markup(chat_id=message.chat.id, message_id=message.message_id, reply_markup=inline_kb)
    except Exception as e:
        logging.exception(f"Error in update_time_in_turku_keyboard: {e}")

def ask_final_confirmation(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Prepare the summary message
        summary = generate_summary(user_id)
        
        # Send the summary
        bot.send_message(
            chat_id,
            f"{messages[lang_code]['submission_summary']}\n\n{summary}"
        )
        
        # Create confirmation inline keyboard
        inline_kb = types.InlineKeyboardMarkup(row_width=2)
        yes_button = types.InlineKeyboardButton(
            text=messages[lang_code]['confirm_submission'], 
            callback_data="confirm_yes"
        )
        modify_button = types.InlineKeyboardButton(
            text=messages[lang_code]['modify_responses'], 
            callback_data="confirm_modify"
        )
        inline_kb.add(yes_button, modify_button)
        
        # Ask for confirmation
        bot.send_message(
            chat_id,
            messages[lang_code]['confirm_responses'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_final_confirmation: {e}")
        lang_code = get_user_language(user_id) if 'user_id' in locals() else 'en'
        bot.send_message(chat_id, messages[lang_code]['error_occurred'])

# Update the summary generation function
def generate_summary(user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        action_type = user_data[user_id]['action_type']
        location_data = user_data[user_id]['location']
        
        summary_parts = []
        
        # Action type and details
        if action_type == 'issue':
            issues = user_data[user_id]['issue_type'] + user_data[user_id].get('custom_issue', [])
            issues_text = ', '.join(issues)
            summary_parts.append(f"{messages[lang_code]['labels']['issue_type']}: {issues_text}")
        elif action_type == 'improvement':
            improvements = user_data[user_id]['improvement_type'] + user_data[user_id].get('custom_improvement', [])
            improvements_text = ', '.join(improvements)
            summary_parts.append(f"{messages[lang_code]['labels']['improvement_type']}: {improvements_text}")
        elif action_type == 'both':
            # Handle both issues and improvements
            issues = user_data[user_id]['issue_type'] + user_data[user_id].get('custom_issue', [])
            improvements = user_data[user_id]['improvement_type'] + user_data[user_id].get('custom_improvement', [])
            
            issues_text = ', '.join(issues)
            improvements_text = ', '.join(improvements)
            
            summary_parts.append(f"{messages[lang_code]['labels']['issue_type']}: {issues_text}")
            summary_parts.append(f"{messages[lang_code]['labels']['improvement_type']}: {improvements_text}")
        
        # Location information
        if location_data.get('venue_title'):
            summary_parts.append(f"{messages[lang_code]['labels']['location']}: {location_data['venue_title']}, {location_data['venue_address']}")
        else:
            summary_parts.append(f"{messages[lang_code]['labels']['location']}: {location_data['latitude']}, {location_data['longitude']}")
        
        # Additional info if provided
        additional_info = user_data[user_id].get('additional_info', '')
        if additional_info:
            summary_parts.append(f"{messages[lang_code]['additional_info_prompt']}: {additional_info}")
        
        # Socioeconomic information if provided
        if user_data[user_id].get('age') and user_data[user_id]['age'] != 'Not provided':
            summary_parts.append(f"{messages[lang_code]['labels']['age']}: {user_data[user_id]['age']}")
        
        if user_data[user_id].get('gender') and user_data[user_id]['gender'] != 'Not provided':
            summary_parts.append(f"{messages[lang_code]['labels']['gender']}: {user_data[user_id]['gender']}")
        
        if user_data[user_id].get('occupation') and user_data[user_id]['occupation'] != 'Not provided':
            summary_parts.append(f"{messages[lang_code]['labels']['occupation']}: {user_data[user_id]['occupation']}")
        
        if user_data[user_id].get('time_in_turku') and user_data[user_id]['time_in_turku'] != 'Not provided':
            summary_parts.append(f"{messages[lang_code]['labels']['time_in_turku']}: {user_data[user_id]['time_in_turku']}")
        
        # Current date and time
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary_parts.append(f"Timestamp: {current_time}")
        
        return '\n'.join(summary_parts)
    except Exception as e:
        logging.exception(f"Error in generate_summary: {e}")
        return "Error generating summary."

# Update the final confirmation handler
@bot.callback_query_handler(func=lambda call: call.data.startswith('confirm_'))
def handle_final_confirmation(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        choice = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        if choice == 'yes':  # Yes, submit
            # Save the data
            success = save_data(chat_id, user_id)
            
            if success:
                # Ask if they want to submit another location
                ask_submit_another(chat_id, user_id)
            else:
                # Error message handled in save_data function
                pass
                
        elif choice == 'modify':  # User wants to modify responses
            # Show modifiable question blocks
            show_modifiable_questions(chat_id, user_id)
            
        elif choice == 'no':  # No, start over
            # Acknowledge the choice
            bot.answer_callback_query(call.id, "Starting over...")
            
            # Send a message and restart
            bot.send_message(
                chat_id,
                "Let's start over."
            )
            
            # Get language before restarting
            saved_language = user_data[user_id].get('language', 'en')
            
            # Restart the process from consent
            # Initialize user data but keep language
            user_data[user_id] = {'language': saved_language}
            
            # Ask for consent
            ask_for_consent(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in handle_final_confirmation: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


def show_modifiable_questions(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Create simplified modification options
        inline_kb = types.InlineKeyboardMarkup(row_width=1)
        
        # Option 1: Modify location
        location_button = types.InlineKeyboardButton(
            text=messages[lang_code]['modify_location'], 
            callback_data="modify_location"
        )
        
        # Option 2: Modify issue/improvement
        action_button = types.InlineKeyboardButton(
            text=messages[lang_code]['modify_action'], 
            callback_data="modify_action"
        )
        
        # Option 3: Update personal information (single option)
        socio_button = types.InlineKeyboardButton(
            text=messages[lang_code]['modify_socio'], 
            callback_data="modify_socio"
        )
        
        # Return to summary button
        done_button = types.InlineKeyboardButton(
            text="🎯 " + messages[lang_code]['done_button'],
            callback_data="modify_done"
        )
        
        # Add all buttons to keyboard
        inline_kb.add(location_button, action_button, socio_button, done_button)
        
        # Send message with options
        bot.send_message(
            chat_id,
            messages[lang_code]['select_questions_to_modify'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in show_modifiable_questions: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('modify_'))
def handle_modify_selection(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        action = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        # Handle different modification options
        if action == 'done':
            # Return to summary and confirmation
            ask_final_confirmation(chat_id, user_id)
            
        elif action == 'location':
            # Set flag to indicate we're in modify mode
            user_data[user_id]['is_modifying'] = True
            # Ask for location again
            bot.answer_callback_query(call.id, "Updating location...")
            ask_location(chat_id, user_id)
            
        elif action == 'action':
            # Set flag to indicate we're in modify mode
            user_data[user_id]['is_modifying'] = True
            # Reset action data
            if 'action_type' in user_data[user_id]:
                user_data[user_id].pop('action_type', None)
            if 'action_types' in user_data[user_id]:
                user_data[user_id].pop('action_types', None)
            if 'issue_type' in user_data[user_id]:
                user_data[user_id].pop('issue_type', None)
            if 'custom_issue' in user_data[user_id]:
                user_data[user_id].pop('custom_issue', None)
            if 'improvement_type' in user_data[user_id]:
                user_data[user_id].pop('improvement_type', None)
            if 'custom_improvement' in user_data[user_id]:
                user_data[user_id].pop('custom_improvement', None)
                
            # Ask for action selection again
            bot.answer_callback_query(call.id, "Updating issue/improvement selection...")
            ask_action_selection(chat_id, user_id)
            
        elif action == 'socio':
            # Set modify flag
            user_data[user_id]['is_modifying'] = True
            # Just start the socioeconomic flow from the beginning
            bot.answer_callback_query(call.id, "Updating personal information...")
            # Go to socioeconomic intro question
            ask_socioeconomic_info(chat_id, user_id)
        
        else:
            # Invalid option
            bot.answer_callback_query(call.id, messages[lang_code]['invalid_selection'])
            show_modifiable_questions(chat_id, user_id)
            
    except Exception as e:
        logging.exception(f"Error in handle_modify_selection: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")

# Update the submit another function
def ask_submit_another(chat_id, user_id):
    try:
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Send thank you message
        bot.send_message(
            chat_id,
            messages[lang_code]['submission_received']
        )
        
        # Create inline keyboard for submit another option
        inline_kb = types.InlineKeyboardMarkup(row_width=2)
        yes_button = types.InlineKeyboardButton(
            text=messages[lang_code]['submit_another_options'][0], 
            callback_data="another_yes"
        )
        no_button = types.InlineKeyboardButton(
            text=messages[lang_code]['submit_another_options'][1], 
            callback_data="another_no"
        )
        inline_kb.add(yes_button, no_button)
        
        # Ask if they want to submit another
        bot.send_message(
            chat_id,
            messages[lang_code]['submit_another'],
            reply_markup=inline_kb
        )
    except Exception as e:
        logging.exception(f"Error in ask_submit_another: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


# Update the submit another handler
@bot.callback_query_handler(func=lambda call: call.data.startswith('another_'))
def handle_submit_another(call):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        choice = call.data.split('_')[1]
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        # Remove the inline keyboard
        bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        
        # Ensure user_id exists in user_data
        if user_id not in user_data:
            user_data[user_id] = {'language': lang_code}
        
        if choice == 'yes':  # Yes, submit another
            # Acknowledge the choice
            bot.answer_callback_query(call.id, messages[lang_code]['submit_another_options'][0])
            
            # Save language, consent and socioeconomic data
            consent = user_data[user_id].get('consent', True)
            age = user_data[user_id].get('age', None)
            gender = user_data[user_id].get('gender', None)
            occupation = user_data[user_id].get('occupation', None)
            time_in_turku = user_data[user_id].get('time_in_turku', None)
            language = user_data[user_id].get('language', 'en')
            
            # Clear user data but keep consent and socioeconomic data
            user_data[user_id] = {
                'consent': consent,
                'age': age,
                'gender': gender,
                'occupation': occupation,
                'time_in_turku': time_in_turku,
                'language': language
            }
            
            # Start from location request
            ask_location(chat_id, user_id)
            
        elif choice == 'no':  # No, I'm done
            # Acknowledge the choice
            bot.answer_callback_query(call.id, messages[lang_code]['submit_another_options'][1])
            
            # Send a thank you message
            bot.send_message(
                chat_id,
                messages[lang_code]['thank_you']
            )
            
            # Return to main menu
            send_main_menu(chat_id, user_id)
    except Exception as e:
        logging.exception(f"Error in handle_submit_another: {e}")
        bot.send_message(chat_id, "An error occurred. Please try again later.")


       
def save_data(chat_id, user_id):
    """
    Save user data to the database, replacing original save_data function
    
    Args:
        chat_id (int): Telegram chat ID
        user_id (int): Telegram user ID
        
    Returns:
        bool: Success status
    """
    try:
        # Generate anonymous ID for the user
        anonymous_id = get_anonymous_user_id(user_id)
        
        # Get action type and location data from user_data dictionary
        action_type = user_data[user_id]['action_type']
        location_data = user_data[user_id]['location']
        additional_info = user_data[user_id].get('additional_info', '')
        
        # Get socioeconomic data
        age = user_data[user_id].get('age', 'Not provided')
        gender = user_data[user_id].get('gender', 'Not provided')
        occupation = user_data[user_id].get('occupation', 'Not provided')
        time_in_turku = user_data[user_id].get('time_in_turku', 'Not provided')
        
        # Update user preferences - only update socioeconomic data if it changed
        # to avoid overwriting with 'Not provided' if previously answered
        existing_data = check_user_socioeconomic_data(anonymous_id)
        
        if existing_data:
            # User already has socioeconomic data, only update consent
            update_user_preferences(
                anonymous_id=anonymous_id,
                consent=user_data[user_id].get('consent', True)
            )
        else:
            # User doesn't have socioeconomic data or chose to update it
            update_user_preferences(
                anonymous_id=anonymous_id,
                consent=user_data[user_id].get('consent', True),
                age=age,
                gender=gender,
                occupation=occupation,
                time_in_turku=time_in_turku
            )
        
        # Handle different action types
        success = True
        
        if action_type == 'both':
            # First, handle issues
            issue_standard_selections = user_data[user_id]['issue_type']
            issue_custom_inputs = user_data[user_id].get('custom_issue', [])
            
            issue_standard_details = ';'.join(issue_standard_selections) if issue_standard_selections else ''
            issue_custom_details = ';'.join(issue_custom_inputs) if issue_custom_inputs else ''
            
            # Save issues
            issue_id = save_submission(
                anonymous_id=anonymous_id,
                submission_type='issue',
                standard_selections=issue_standard_details,
                custom_inputs=issue_custom_details,
                latitude=location_data['latitude'],
                longitude=location_data['longitude'],
                venue_title=location_data.get('venue_title', ''),
                venue_address=location_data.get('venue_address', ''),
                additional_info=additional_info
            )
            
            success = success and issue_id > 0
            
            # Second, handle improvements
            improvement_standard_selections = user_data[user_id]['improvement_type']
            improvement_custom_inputs = user_data[user_id].get('custom_improvement', [])
            
            improvement_standard_details = ';'.join(improvement_standard_selections) if improvement_standard_selections else ''
            improvement_custom_details = ';'.join(improvement_custom_inputs) if improvement_custom_inputs else ''
            
            # Save improvements
            improvement_id = save_submission(
                anonymous_id=anonymous_id,
                submission_type='improvement',
                standard_selections=improvement_standard_details,
                custom_inputs=improvement_custom_details,
                latitude=location_data['latitude'],
                longitude=location_data['longitude'],
                venue_title=location_data.get('venue_title', ''),
                venue_address=location_data.get('venue_address', ''),
                additional_info=additional_info
            )
            
            success = success and improvement_id > 0
        else:
            # Handle single submission types (issue or improvement)
            
            # Separate standard selections from custom inputs
            if action_type == 'issue':
                submission_type = 'issue'
                standard_selections = user_data[user_id]['issue_type']
                custom_inputs = user_data[user_id].get('custom_issue', [])
            else:  # improvement
                submission_type = 'improvement'
                standard_selections = user_data[user_id]['improvement_type']
                custom_inputs = user_data[user_id].get('custom_improvement', [])
            
            # Join all selections with semicolons to maintain format
            standard_details = ';'.join(standard_selections) if standard_selections else ''
            custom_details = ';'.join(custom_inputs) if custom_inputs else ''
            
            # Save submission
            submission_id = save_submission(
                anonymous_id=anonymous_id,
                submission_type=submission_type,
                standard_selections=standard_details,
                custom_inputs=custom_details,
                latitude=location_data['latitude'],
                longitude=location_data['longitude'],
                venue_title=location_data.get('venue_title', ''),
                venue_address=location_data.get('venue_address', ''),
                additional_info=additional_info
            )
            
            success = submission_id > 0
        
        return success
    except Exception as e:
        logging.exception(f"Error in save_data: {e}")
        flow_logger.error(f"Save data failed: {e}")
        bot.send_message(chat_id, "An error occurred while saving your data. Please try again later.")
        return False

# Handle text messages for custom inputs
@bot.message_handler(func=lambda m: True, content_types=['text'])
def handle_text_input(message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        text = message.text.strip()
        
        # Get user's language
        lang_code = get_user_language(user_id)
        
        if user_id in user_data and 'awaiting_multiple_select' in user_data[user_id]:
            mode = user_data[user_id]['awaiting_multiple_select']
            
            if mode == 'issue':
                # Initialize custom_issue if needed
                if 'custom_issue' not in user_data[user_id]:
                    user_data[user_id]['custom_issue'] = []
                
                # Add the text input to custom issues
                user_data[user_id]['custom_issue'].append(text)
                
                # Confirmation message
                bot.reply_to(message, messages[lang_code]['free_text_added'])
                
            elif mode == 'improvement':
                # Initialize custom_improvement if needed
                if 'custom_improvement' not in user_data[user_id]:
                    user_data[user_id]['custom_improvement'] = []
                
                # Add the text input to custom improvements
                user_data[user_id]['custom_improvement'].append(text)
                
                # Confirmation message
                bot.reply_to(message, messages[lang_code]['free_text_added'])
            
            elif mode == 'action':
                # Custom actions are not allowed
                bot.reply_to(message, messages[lang_code]['please_select_at_least_one'])
                
            else:
                # Unknown mode, just restart
                bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        else:
            # Check if awaiting additional info
            if user_id in user_data and 'additional_info' in user_data.get(user_id, {}):
                # Store the additional info and proceed
                user_data[user_id]['additional_info'] = text
                
                # Proceed to socioeconomic info
                ask_socioeconomic_info(chat_id, user_id)
            else:
                # Not awaiting any input, suggest using /start
                bot.send_message(chat_id, "Please use /start to begin using this bot.")
    except Exception as e:
        logging.exception(f"Error in handle_text_input: {e}")
        # Try to get user's language for error message
        try:
            lang_code = get_user_language(user_id)
            bot.send_message(chat_id, messages[lang_code]['error_occurred'])
        except:
            # Fallback to English
            bot.send_message(chat_id, "An error occurred. Please try again later.")

# Start the bot
if __name__ == '__main__':
    # Initialize the database
    initialize_database()
    
    # Initialize the connection pool
    initialize_connection_pool()
    
    while True:
        try:
            bot.polling(none_stop=True)
        except Exception as e:
            logging.exception(f"Bot polling failed: {e}")
            time.sleep(15)





