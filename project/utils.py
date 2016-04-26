import os, subprocess, time, sys

ROOT = "/home/ubuntu/project"
LISTENER = os.path.join(ROOT, "listener_tweepy.py")
JAR = os.path.join(ROOT, "storm/target/storm-project-0.9.3-jar-with-dependencies.jar")
APP = "com.mycompany.app.App"
TOPOLOGY = "mytopology"
WAITSECS = "0"

LISTENER2 = None

def launchListener(keyword):
    # launch listener
    global LISTENER2

    if LISTENER2 != None:
        LISTENER2.kill()

    args = [LISTENER, keyword]
    print args
    LISTENER2 = subprocess.Popen(args,\
            stdout=subprocess.PIPE,\
            stderr=subprocess.PIPE)

    print "launched listener succeeded"
    print "pid: ", LISTENER2.pid
    return 1

def getListener():
    return LISTENER2

def launchStorm():
    try:
        output = subprocess.check_output(['storm', "jar", JAR, APP, TOPOLOGY])
        print "launch storm succeeded"
        return 1
    except:
        print "launch failed"
        return 0

def killStorm():
    try:
        output = subprocess.check_output(['storm', "kill", TOPOLOGY, '-w', WAITSECS])
        return 1
    except:
        return 0

def test():
    try:
        kill()
    except:
        print "kill failed"
    time.sleep(3)
    launch("game")
