import pyttsx3


def talkbox(phrase, v):
    engine = pyttsx3.init() # object creation

    """ RATE"""
    rate = engine.getProperty('rate')   # getting details of current speaking rate
    engine.setProperty('rate', 125)     # setting up new voice rate


    """VOLUME"""
    volume = engine.getProperty('volume')   #getting to know current volume level (min=0 and max=1)                      #printing current volume level
    engine.setProperty('volume', .75)    # setting up volume level  between 0 and 1

    """VOICE"""
    voices = engine.getProperty('voices')       #getting details of current voice
    engine.setProperty('voice', voices[v].id)   #changing index, changes voices. o for male | 1 for female

    engine.say(phrase)
    engine.runAndWait()
    engine.stop()