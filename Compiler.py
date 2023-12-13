import csv
import json
import sys

from vincenty import vincenty
import progressbar
from configparser import ConfigParser
import constants as c
from Wikipedia import Wikipedia
from TZData import TZData
from TEBCCWeatherKitProvider import TEBCCWeatherKitProvider
from GeoNamesWrapper import GeoNamesWrapper
import os
from datetime import datetime, timedelta
import math
import glob
import time


# Higher level class that takes care of a lot of the compilation shtuff.
class Compiler:
    def __init__(self, tsv_infile, tsv_outfile, json_outfile, configfile):
        self.config = None
        self.tsv_infile = tsv_infile
        self.tsv_outfile = tsv_outfile
        self.json_outfile = json_outfile
        self.r = {"destinations": [], "generated": int(time.time())}
        self.route = self.r['destinations']
        self.configfile = configfile
        self.configparser = ConfigParser()
        self.configparser.read(self.configfile)

        self.errors = 0
        self.warnings = 0

        self.offset = self.configparser.getint("COMPILER", "starttime") - self.configparser.getint("COMPILER",
                                                                                                   "actualruntime")
        if os.getenv("CI_COMMIT_BRANCH") == "dev" and not self.configparser.getboolean("COMPILER", "override_auto_devstart"):
            # Automatically set the dev branch to get a run going at 1:59:55 AM
            # This is not done if we are forcing a dev branch run to happen at a specific time.
            # If it is before 1 AM, then we go back a day and start the run then
            nowdt = datetime.now()
            nowdt_sid = (nowdt.hour * 3600) + (nowdt.minute * 60) + nowdt.second
            startdt = nowdt.replace(hour=2, minute=0, second=0)
            if nowdt_sid < 6300:
                startdt = startdt - timedelta(days=1)
            self.offset = self.configparser.getint("COMPILER", "starttime") - math.floor(startdt.timestamp())
        elif os.getenv("CI_COMMIT_BRANCH") == "staging" and not self.configparser.getboolean("COMPILER", "override_auto_mainstart"):
            self.offset = 0
        elif os.getenv("CI_COMMIT_BRANCH") == "main" and not self.configparser.getboolean("COMPILER", "override_auto_mainstart"):
            # Similarly, this ensures that any route being compiled for production is done so at the intended time.
            self.offset = 0
            if self.configparser.getboolean("WEATHERKIT", "dryrun"):
                print("!!! HEY DINGUS !!!")
                print("DRY RUN IS ENABLED AND YOU'RE TRYING TO COMPILE A ROUTE TO PROD IN CI")
                print("THEREFORE COMPILATION IS STOPPING WITH ERROR CODE 69 AND YOU NEED TO CHECK FOR STUPID ERRORS")
                print("!!! HEY DINGUS !!!")
                sys.exit(69)

    # Method that loops through everything, fetches TZData/Wikipedia if requested, and yoinks out a JSON file.
    def compiler(self):
        with open(self.tsv_infile) as inf:
            length = sum(1 for _ in inf)

        with open(self.tsv_infile) as inf, open(self.tsv_outfile, "w", newline='') as outf:
            tsv_reader = csv.reader(inf, delimiter="\t", quotechar='"')
            tsv_writer = csv.writer(outf, delimiter="\t", quotechar='"')
            index = 0
            tempcont = 0
            distance_travelled_km = 0
            distance_travelled_mi = 0

            progressbar.streams.wrap_stdout()
            progressbar.streams.wrap_stderr()

            bar_widgets = [progressbar.PercentageLabelBar(), ' ', progressbar.Counter(), f'/{length - 1} | ',
                           progressbar.Timer(), ' | ', progressbar.ETA()]
            bar = progressbar.ProgressBar(max_value=(length - 1), widgets=bar_widgets)

            wikipedia = Wikipedia()
            tzdata = TZData(self.configparser.getint("TZ", "processingtime"), self.configparser.get("TZ", "apikey"))
            wkp = TEBCCWeatherKitProvider()
            gnw = GeoNamesWrapper(self.configparser.get("GEONAMES", "username"))

            for row in tsv_reader:
                if tempcont == 0:
                    tempcont = 1
                    tsv_writer.writerow(row)
                    continue

                self.route.append({})
                self.route[index]["unixarrival_v2"] = int(row[c.COLUMN_UNIXARRIVAL_ARRIVAL]) - self.offset
                self.route[index]["unixarrival"] = int(row[c.COLUMN_UNIXARRIVAL]) - self.offset
                self.route[index]["unixdeparture"] = int(row[c.COLUMN_UNIXDEPARTURE]) - self.offset
                self.route[index]["city"] = row[c.COLUMN_CITY]
                self.route[index]["region"] = row[c.COLUMN_REGION]
                self.route[index]["countrycode"] = row[c.COLUMN_COUNTRYCODE]
                if self.configparser.getboolean("TOBCC", "tobcc_mode"):
                    self.route[index]["eggsdelivered"] = int(int(row[c.COLUMN_BASKETSDELIVERED]) / 5)
                    self.route[index]["carrotseaten"] = int(int(row[c.COLUMN_CARROTSEATEN]) / 5)
                else:
                    self.route[index]["eggsdelivered"] = int(row[c.COLUMN_BASKETSDELIVERED])
                    self.route[index]["carrotseaten"] = int(row[c.COLUMN_CARROTSEATEN])
                self.route[index]["lat"] = float(row[c.COLUMN_LATITUDE])
                self.route[index]["lng"] = float(row[c.COLUMN_LONGITUDE])
                self.route[index]["population"] = int(row[c.COLUMN_POPULATION])
                self.route[index]["population_year"] = str(row[c.COLUMN_POPULATIONYEAR])
                self.route[index]["elevation"] = int(row[c.COLUMN_ELEVATION])

                # Do Wikipedia processing here
                row[c.COLUMN_WIKIPEDIALINK] = row[c.COLUMN_WIKIPEDIALINK].replace("#Climate", "")
                self.route[index]["srclink"] = row[c.COLUMN_WIKIPEDIALINK]
                # Here's how this works. If Wikipedia is set to use, any empty description is automatically fetched.
                # If Wikipedia Use is on and force fetch is on, it'll get the description for any row.
                # This auto-includes regex parsing.

                # If cleanup is set to True (separate), then just a standard non-regexy parsing happens.
                if self.configparser.getboolean("WIKIPEDIA", "use"):
                    if row[c.COLUMN_WIKIPEDIADESCR] == "" or self.configparser.getboolean("WIKIPEDIA", "force_fetch"):
                        if row[c.COLUMN_WIKIPEDIALINK] != "":
                            row[c.COLUMN_WIKIPEDIADESCR] = wikipedia.fetch(row[c.COLUMN_WIKIPEDIALINK].split("/")[-1])
                            try:
                                row[c.COLUMN_WIKIPEDIADESCR] = wikipedia.regex_parse(row[c.COLUMN_WIKIPEDIADESCR])
                            except TimeoutError:
                                self.printer("WARNING", index, "Wikipedia regex timed out for this row.")
                                pass

                if self.configparser.getboolean("WIKIPEDIA", "cleanup"):
                    if row[c.COLUMN_WIKIPEDIALINK] != "":
                        row[c.COLUMN_WIKIPEDIADESCR] = wikipedia.general_parse(row[c.COLUMN_WIKIPEDIADESCR])

                self.route[index]["descr"] = row[c.COLUMN_WIKIPEDIADESCR]
                # Do Google TZ Processing here
                if self.configparser.getboolean("TZ", "use"):
                    if row[c.COLUMN_TIMEZONE] == "" or self.configparser.getboolean("TZ", "force_fetch"):
                        row[c.COLUMN_TIMEZONE] = tzdata.fetch(row[c.COLUMN_LATITUDE], row[c.COLUMN_LONGITUDE])

                self.route[index]["timezone"] = row[c.COLUMN_TIMEZONE]

                # Do Dark Sky processing here
                if self.route[index]["region"] != "pt":
                    weatherdata = wkp.request_tebcc(lat=self.route[index]["lat"],
                                                    lng=self.route[index]["lng"],
                                                    time=self.route[index]["unixarrival"],
                                                    dryrun=self.configparser.getboolean("WEATHERKIT", "dryrun"))
                    self.route[index]["weather"] = {
                        "tempC": weatherdata['temperature'],
                        "tempF": weatherdata['temperatureF'],
                        "summary": weatherdata['conditionCode'],
                        "icon": weatherdata['icon']
                    }
                else:
                    self.route[index]["weather"] = {
                        "tempC": 70,
                        "tempF": 20,
                        "summary": "Clear",
                        "icon": "wi-night-clear"
                    }

                # Do GeoNames parsing here (with a 1 second delay as to not trigger rate limits)
                if self.configparser.getboolean("GEONAMES", "use"):
                    if row[c.COLUMN_LOCALE] == "" or self.configparser.getboolean("GEONAMES", "force_fetch"):
                        row[c.COLUMN_LOCALE] = gnw.fetch(lat=self.route[index]["lat"],
                                                         lng=self.route[index]["lng"])

                self.route[index]["locale"] = row[c.COLUMN_LOCALE]
                if self.route[index]["locale"] == "None":
                    self.route[index]["locale"] = ""

                # Do vincenty equations here (no more round 2)
                try:
                    point1 = (float(self.route[index - 1]["lat"]),
                              float(self.route[index - 1]["lng"]))
                    point2 = (float(self.route[index]["lat"]),
                              float(self.route[index]["lng"]))
                    temp_travelled_km = vincenty(point1, point2)
                    temp_travelled_mi = vincenty(point1, point2, miles=True)
                    distance_travelled_km += temp_travelled_km
                    distance_travelled_mi += temp_travelled_mi
                    self.route[index]["distance-km"] = round(distance_travelled_km, 4)
                    self.route[index]["distance-mi"] = round(distance_travelled_mi, 4)
                except KeyError:
                    self.printer("WARNING", index, "Distance calculation failed for this row.")
                    self.route[index]["distance-km"] = round(distance_travelled_km, 4)
                    self.route[index]["distance-mi"] = round(distance_travelled_mi, 4)

                try:
                    point1 = (float(self.route[index - 1]["lat"]),
                              float(self.route[index - 1]["lng"]))
                    point2 = (float(self.route[index]["lat"]),
                              float(self.route[index]["lng"]))
                    speed_travelled_km = vincenty(point1, point2)
                    speed_travelled_mi = vincenty(point1, point2, miles=True)
                    delta = float(self.route[index]["unixarrival_v2"]) - float(self.route[index - 1]["unixdeparture"])
                    self.route[index - 1]["speed-kph"] = round((speed_travelled_km / delta) * 3600, 4)
                    self.route[index - 1]["speed-mph"] = round((speed_travelled_mi / delta) * 3600, 4)
                except (ZeroDivisionError, KeyError):
                    self.printer("WARNING", (index - 1), "Speed calculation failed for this row.")
                    self.route[index]["speed-kph"] = 0
                    self.route[index]["speed-mph"] = 0

                # And finish things off with writing the row.
                tsv_writer.writerow(row)
                index += 1
                bar.update(index)

        self.route[index - 1]["speed-kph"] = 0
        self.route[index - 1]["speed-mph"] = 0

        with open(self.json_outfile, "w") as json_out:
            json.dump(self.r, json_out)

        # Update .env.development file
        now = datetime.now()
        now_str = now.strftime("%Y%m%d")
        with open("../.env.development", "a") as dev_env:
            dev_env.write(f"\nREACT_APP_VERSION=v{now_str}") 

        for file in glob.glob("../.env*"):
            print(f"Found: {file}")
            if ".env.development.local" in file:
                continue

            with open(file, "a") as f:
                f.write(f"\nREACT_APP_COMMIT={os.getenv('CI_COMMIT_SHORT_SHA')}")


        

    def printer(self, severity, row, message):
        print(f"{severity} - Row {row} - {message}")
        if severity == "ERROR":
            self.errors += 1
        elif severity == "WARNING":
            self.warnings += 1

    # Validator method validates the route.tsv file to ensure it's not goobed.
    def validator(self):
        with open(self.tsv_infile) as fc:
            rd = csv.reader(fc, delimiter="\t", quotechar='"')
            index = 0
            tempcont = 0
            prev_baskets = 0
            prev_carrots = 0
            prev_timestamp_arrival = 0
            prev_timestamp = 0
            prev_timestamp_departure = 0
            prev_latitude = 0
            prev_longitude = 0

            for row in rd:
                if tempcont == 0:
                    tempcont = 1
                    continue

                if row[c.COLUMN_UNIXARRIVAL_ARRIVAL] == "":
                    self.printer("ERROR", index, "Arrival arrival timestamp is missing!")
                else:
                    try:
                        unixarrival_arrival = int(row[c.COLUMN_UNIXARRIVAL_ARRIVAL])
                        if prev_timestamp_arrival > unixarrival_arrival:
                            self.printer("ERROR", index, "Unix arrival arrival has gone backwards!")

                        prev_timestamp_arrival = unixarrival_arrival
                    except ValueError:
                        self.printer("ERROR", index, "Unix arrival arrival is an invalid type!")

                if row[c.COLUMN_UNIXARRIVAL] == "":
                    self.printer("ERROR", index, "Arrival timestamp is missing!")
                else:
                    try:
                        unixarrival = int(row[c.COLUMN_UNIXARRIVAL])
                        if prev_timestamp > unixarrival:
                            self.printer("ERROR", index, "Unix arrival has gone backwards!")

                        if not prev_timestamp - 600 <= unixarrival:
                            self.printer("WARNING", index, "Previous unix arrival has a diff of 10+ minutes")

                        prev_timestamp = unixarrival
                    except ValueError:
                        self.printer("ERROR", index, "Unix arrival is an invalid type!")

                if row[c.COLUMN_UNIXDEPARTURE] == "":
                    self.printer("ERROR", index, "Departure timestamp is missing!")
                else:
                    try:
                        unixdeparture = int(row[c.COLUMN_UNIXDEPARTURE])
                        if prev_timestamp_departure > unixdeparture:
                            self.printer("ERROR", index, "Unix departure has gone backwards!")

                        prev_timestamp_departure = unixdeparture
                    except ValueError:
                        self.printer("ERROR", index, "Unix departure is an invalid type!")

                if row[c.COLUMN_CITY] == "":
                    self.printer("ERROR", index, "City name is missing!")

                if row[c.COLUMN_REGION] == "":
                    self.printer("ERROR", index, "Region is missing!")

                if row[c.COLUMN_COUNTRYCODE] == "" and row[c.COLUMN_REGION] != "pt":
                    self.printer("ERROR", index, "Country code is missing for a non-pretracking stop!")

                if row[c.COLUMN_BASKETSDELIVERED] == "":
                    self.printer("ERROR", index, "Baskets delivered is missing!")
                else:
                    try:
                        int(row[c.COLUMN_BASKETSDELIVERED])
                        if prev_baskets > int(row[c.COLUMN_BASKETSDELIVERED]):
                            self.printer("ERROR", index, "Baskets delivered has decreased for this row!")

                        prev_baskets = int(row[c.COLUMN_BASKETSDELIVERED])
                    except ValueError:
                        self.printer("ERROR", index, "Baskets delivered is an invalid type!")

                if row[c.COLUMN_CARROTSEATEN] == "":
                    self.printer("ERROR", index, "Carrots eaten is missing!")
                else:
                    try:
                        int(row[c.COLUMN_CARROTSEATEN])
                        if prev_carrots > int(row[c.COLUMN_CARROTSEATEN]):
                            self.printer("ERROR", index, "Carrots eaten has decreased for this row!")

                        prev_carrots = int(row[c.COLUMN_CARROTSEATEN])
                    except ValueError:
                        self.printer("ERROR", index, "Carrotes eaten is an invalid type!")

                if row[c.COLUMN_LATITUDE] == "":
                    self.printer("ERROR", index, "Latitude is missing!")
                else:
                    try:
                        float(row[c.COLUMN_LATITUDE])
                        if abs(float(row[c.COLUMN_LATITUDE]) - prev_latitude) > 100:
                            self.printer("WARNING", index,
                                         "The latitude change between this and the last stop is abnormally high! (%s degrees)" % str(
                                             round(float(row[c.COLUMN_LATITUDE]), 2) - prev_latitude))

                        prev_latitude = float(row[c.COLUMN_LATITUDE])
                    except ValueError:
                        self.printer("ERROR", index, "Latitude is not a valid type!")

                if row[c.COLUMN_LONGITUDE] == "":
                    self.printer("ERROR", index, "Longitude is missing!")
                else:
                    try:
                        float(row[c.COLUMN_LONGITUDE])
                        if abs(float(row[c.COLUMN_LONGITUDE]) - prev_longitude) > 100:
                            self.printer("WARNING", index,
                                         "The longitude change between this and the last stop is abnormally high! (%s degrees)" % str(
                                             round(float(row[c.COLUMN_LONGITUDE]), 2) - prev_longitude))

                        prev_longitude = float(row[c.COLUMN_LONGITUDE])
                    except ValueError:
                        self.printer("ERROR", index, "Longitude is not a valid type!")

                if row[c.COLUMN_POPULATION] == "":
                    self.printer("ERROR", index, "Population number is missing!")
                else:
                    try:
                        int(row[c.COLUMN_POPULATION])
                    except ValueError:
                        self.printer("ERROR", index, "Population number is not a valid type!")

                if row[c.COLUMN_POPULATIONYEAR] == "":
                    self.printer("ERROR", index, "Population year is missing!")
                else:
                    try:
                        int(row[c.COLUMN_POPULATIONYEAR])
                    except ValueError:
                        self.printer("ERROR", index, "Population year is not a valid type!")

                    if row[c.COLUMN_POPULATIONYEAR] == "0" and row[c.COLUMN_REGION] != "pt":
                        self.printer("WARNING", index,
                                     "Population year is 0. The tracker will not show population year for this stop.")

                if row[c.COLUMN_ELEVATION] == "":
                    self.printer("ERROR", index, "Elevation is missing!")
                else:
                    try:
                        int(row[c.COLUMN_ELEVATION])
                    except ValueError:
                        self.printer("ERROR", index, "Elevation is not a valid type!")

                if row[c.COLUMN_TIMEZONE] == "" and row[c.COLUMN_CITY] != self.configparser.get("COMPILER",
                                                                                                "basestop_cityname") and not self.configparser.getboolean(
                        "TZ", "use"):
                    self.printer("ERROR", index, "Timezone is missing and fetching TZ data is off!")

                if row[c.COLUMN_WIKIPEDIALINK] == "" and row[c.COLUMN_REGION] != "pt" and row[
                    c.COLUMN_CITY] != self.configparser.get("COMPILER", "basestop_cityname"):
                    self.printer("ERROR", index, "Wikipedia link missing!")

                if row[c.COLUMN_WIKIPEDIADESCR] == "" and row[c.COLUMN_REGION] != "pt" and row[
                    c.COLUMN_CITY] != self.configparser.get("COMPILER",
                                                            "basestop_cityname") and not self.configparser.getboolean(
                        "WIKIPEDIA", "use"):
                    self.printer("ERROR", index, "Wikipedia description is missing and fetching Wikipedia "
                                                 "descriptions is off!")

                index = index + 1