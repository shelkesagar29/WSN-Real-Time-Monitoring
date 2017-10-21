"""Author: Sagar Shelke

This is a python application to monitor WSN data in Real time and assist in data collection process for ML project.
We are using 20 ultrasound sensors in a grid and plotting their data in real time.

Environment we built form a rectangle as shown below,
               max 280
(0,0)| - - - - - - - - - - - - - - - -- X
     |
max  |
250  |
     |
     |
     |
     Y
Sensor data is being plotted w.r.t time

We use MQTT IoT protocol to send data over internet using JSON. (To be replaced with Google Protocol buffers to make it
language neutral )

This module has 3 classes
PlotSensorData: This class uses shared variables to plot data with respect to time
PlotPosition : This class localizes object within the environment
RunMqtt: This class runs MQTT client continuously to fetch data

Why Multiprocessing?
Aim was to plot data in real time. To do this we need two things to happen together, fetch data from MQTT server and
plot it using matplotlib plotting library.

Since plotting with matplotlib is a blocking operation, we had option to choose either threading or processing.

Threading: This is easy because all threads within a process share a common memory space. BUT problem is matplotlib
does not run as a thread.(Since all threads were not intended for CPU operation, GIL is NOT an issue.)

Thus we moved to multiprocessing by sharing(passing) data structures to each process.

What processes are running?
1. Fetch data from MQTT server
2. Plot data in real time
3. Plot Position

"""

import paho.mqtt.client as mqtt
import json
import time
from matplotlib import animation
from matplotlib.pylab import *
import pandas as pd
import multiprocessing
import datetime
import csv

mqttHost = "iot.eclipse.org"

data_class = "one_dpps"

allData = {}
xpiReceived = 0
ypiReceived = 0
plot_list = []

y_keys = ["x7", "x6", "x5", "x4", "x3", "x2", "x1", "x0"]
x_keys = ["y1", "y2", "y3", "y4", "y5", "y6", "y7", "y8", "y9"]

csv_columns = ['timestamp', 'x0', 'x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8', 'y0', 'y1', 'y2', 'y3', 'y4', 'y5',
               'y6', 'y7', 'y8', 'y9', 'microwave0', 'microwave1', 'PIR0', 'PIR1', 'PIR2', 'PIR3', 'pz0', 'pz1', 'pz2',
               'pz3']
x_pos = []
y_pos = []

write_header = True


class PlotSensorData(object):
    """This class plot sensor data in real time
    Arguments
    ----------
    timestamp_list: a list with five latest timestamps
    x_sensor_data_dict: dictionary with sensor identifier as key and list of five latest values of that sensor as
                       value. (for sensors mounted on x axis)
    y_sensor_data_dict: dictionary with sensor identifier as key and list of five latest values of that sensor as
                       value. (for sensors mounted on y axis)

    Methods
    -------
    update: update plot
    run_data_plotter: run matplotlib animation forever

     """
    def __init__(self, timestamp_list, x_sensor_data_dict, y_sensor_data_dict):
        self.timestamp_list = timestamp_list
        self.x_sensor_data_dict = x_sensor_data_dict
        self.y_sensor_data_dict = y_sensor_data_dict

        self.fig = figure()
        self.fig.suptitle("Real Time Monitoring (Early Beta)", fontsize=12)
        plt.gcf().autofmt_xdate()

        # X axis goes here
        self.ax01 = subplot2grid((5, 4), (0, 0))
        self.ax01.set_ylim(0, 300)
        self.ax01.tick_params(axis='x', labelsize=6)

        self.ax02 = subplot2grid((5, 4), (0, 1))
        self.ax02.set_ylim(0, 300)
        self.ax02.tick_params(axis='x', labelsize=6)

        self.ax03 = subplot2grid((5, 4), (1, 0))
        self.ax03.set_ylim(0, 300)
        self.ax03.tick_params(axis='x', labelsize=6)

        self.ax04 = subplot2grid((5, 4), (1, 1))
        self.ax04.set_ylim(0, 300)
        self.ax04.tick_params(axis='x', labelsize=6)

        self.ax05 = subplot2grid((5, 4), (2, 0))
        self.ax05.set_ylim(0, 300)
        self.ax05.tick_params(axis='x', labelsize=6)

        self.ax06 = subplot2grid((5, 4), (2, 1))
        self.ax06.set_ylim(0, 300)
        self.ax06.tick_params(axis='x', labelsize=6)

        self.ax07 = subplot2grid((5, 4), (3, 0))
        self.ax07.set_ylim(0, 300)
        self.ax07.tick_params(axis='x', labelsize=6)

        self.ax08 = subplot2grid((5, 4), (3, 1))
        self.ax08.set_ylim(0, 300)
        self.ax08.tick_params(axis='x', labelsize=6)

        self.ax09 = subplot2grid((5, 4), (4, 0))
        self.ax09.set_ylim(0, 300)
        self.ax09.tick_params(axis='x', labelsize=6)

        # Y axis goes here
        self.ax10 = subplot2grid((5, 4), (0, 2))
        self.ax10.set_ylim(0, 300)
        self.ax10.tick_params(axis='x', labelsize=6)

        self.ax11 = subplot2grid((5, 4), (0, 3))
        self.ax11.set_ylim(0, 300)
        self.ax11.tick_params(axis='x', labelsize=6)

        self.ax12 = subplot2grid((5, 4), (1, 2))
        self.ax12.set_ylim(0, 300)
        self.ax12.tick_params(axis='x', labelsize=6)

        self.ax13 = subplot2grid((5, 4), (1, 3))
        self.ax13.set_ylim(0, 300)
        self.ax13.tick_params(axis='x', labelsize=6)

        self.ax14 = subplot2grid((5, 4), (2, 2))
        self.ax14.set_ylim(0, 300)
        self.ax14.tick_params(axis='x', labelsize=6)

        self.ax15 = subplot2grid((5, 4), (2, 3))
        self.ax15.set_ylim(0, 300)
        self.ax15.tick_params(axis='x', labelsize=6)

        self.ax16 = subplot2grid((5, 4), (3, 2))
        self.ax16.set_ylim(0, 300)
        self.ax16.tick_params(axis='x', labelsize=6)

        self.ax17 = subplot2grid((5, 4), (3, 3))
        self.ax17.set_ylim(0, 300)
        self.ax17.tick_params(axis='x', labelsize=6)

        # Plotting object for x axis
        self.p011, = self.ax01.plot(x_pos, y_pos, 'b-', label="x1", marker="o")
        self.p012, = self.ax02.plot(x_pos, y_pos, 'b-', label="x2", marker="o")
        self.p021, = self.ax03.plot(x_pos, y_pos, 'b-', label="x3", marker="o")
        self.p022, = self.ax04.plot(x_pos, y_pos, 'b-', label="x4", marker="o")
        self.p031, = self.ax05.plot(x_pos, y_pos, 'b-', label="x5", marker="o")
        self.p032, = self.ax06.plot(x_pos, y_pos, 'b-', label="x6", marker="o")
        self.p041, = self.ax07.plot(x_pos, y_pos, 'b-', label="x7", marker="o")
        self.p042, = self.ax08.plot(x_pos, y_pos, 'b-', label="x8", marker="o")
        self.p051, = self.ax09.plot(x_pos, y_pos, 'b-', label="x9", marker="o")
        # Plotting object for y axis
        self.p013, = self.ax10.plot(x_pos, y_pos, 'r-', label="y1", marker="o")
        self.p014, = self.ax11.plot(x_pos, y_pos, 'r-', label="y2", marker="o")
        self.p023, = self.ax12.plot(x_pos, y_pos, 'r-', label="y3", marker="o")
        self.p024, = self.ax13.plot(x_pos, y_pos, 'r-', label="y4", marker="o")
        self.p033, = self.ax14.plot(x_pos, y_pos, 'r-', label="y5", marker="o")
        self.p034, = self.ax15.plot(x_pos, y_pos, 'r-', label="y6", marker="o")
        self.p043, = self.ax16.plot(x_pos, y_pos, 'r-', label="y7", marker="o")
        self.p044, = self.ax17.plot(x_pos, y_pos, 'r-', label="y8", marker="o")

    def update(self, i):
        time.sleep(1)
        x_axis = [datetime.datetime.strptime(date, "%H:%M:%S") for date in self.timestamp_list]

        # Set data for x axis
        self.p011.set_data(x_axis, self.x_sensor_data_dict["y1"])
        self.p012.set_data(x_axis, self.x_sensor_data_dict["y2"])
        self.p021.set_data(x_axis, self.x_sensor_data_dict["y3"])
        self.p022.set_data(x_axis, self.x_sensor_data_dict["y4"])
        self.p031.set_data(x_axis, self.x_sensor_data_dict["y5"])
        self.p032.set_data(x_axis, self.x_sensor_data_dict["y6"])
        self.p041.set_data(x_axis, self.x_sensor_data_dict["y7"])
        self.p042.set_data(x_axis, self.x_sensor_data_dict["y8"])
        self.p051.set_data(x_axis, self.x_sensor_data_dict["y9"])

        self.p011.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p011.axes.legend(loc='upper right')
        self.p012.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p012.axes.legend(loc='upper right')
        self.p021.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p021.axes.legend(loc='upper right')
        self.p022.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p022.axes.legend(loc='upper right')
        self.p031.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p031.axes.legend(loc='upper right')
        self.p032.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p032.axes.legend(loc='upper right')
        self.p041.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p041.axes.legend(loc='upper right')
        self.p042.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p042.axes.legend(loc='upper right')
        self.p051.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p051.axes.legend(loc='upper right')

        # Set data for y axis
        self.p013.set_data(x_axis, self.y_sensor_data_dict["x7"])
        self.p014.set_data(x_axis, self.y_sensor_data_dict["x6"])
        self.p023.set_data(x_axis, self.y_sensor_data_dict["x5"])
        self.p024.set_data(x_axis, self.y_sensor_data_dict["x4"])
        self.p033.set_data(x_axis, self.y_sensor_data_dict["x3"])
        self.p034.set_data(x_axis, self.y_sensor_data_dict["x2"])
        self.p043.set_data(x_axis, self.y_sensor_data_dict["x1"])
        self.p044.set_data(x_axis, self.y_sensor_data_dict["x0"])

        self.p013.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p013.axes.legend(loc='upper right')
        self.p014.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p014.axes.legend(loc='upper right')
        self.p023.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p023.axes.legend(loc='upper right')
        self.p024.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p024.axes.legend(loc='upper right')
        self.p033.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p033.axes.legend(loc='upper right')
        self.p034.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p034.axes.legend(loc='upper right')
        self.p043.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p043.axes.legend(loc='upper right')
        self.p044.axes.set_xlim(x_axis[0], x_axis[-1])
        self.p044.axes.legend(loc='upper right')

        return self.p011, self.p012, self.p021, self.p022, self.p031, self.p032, self.p041, self.p042, \
               self.p051, self.p013, self.p014, self.p023, self.p024, self.p033, self.p034, \
               self.p043, self.p044,

    def run_data_plotter(self):
        anim = animation.FuncAnimation(self.fig, self.update, frames=200, interval=20)
        plt.show()


class PlotPosition(object):
    """This class plot the position of person in real time

    Arguments
    ---------
    list_x : x axis points to plot(this is a python list)
    list_y : y axis points to plot (this is python list)

    Both the variables are calculated by decide_pos method from RunMqtt class

    Methods
    --------
    animate: set data in real time
    run_plot : matplotlib animation function running forever

    """
    def __init__(self, list_x, list_y):
        self.list_x = list_x  # considered x
        self.list_y = list_y
        self.fig = plt.figure()
        self.fig.suptitle("Object Localization(Early Beta)", fontsize=12)
        self.ax = plt.axes(xlim=(0, 300), ylim=(0, 250))
        self.g, = self.ax.plot(x_pos, y_pos, "go")
        self.r, = self.ax.plot(x_pos, y_pos, "ro")
        self.ax.xaxis.tick_top()
        self.ax.set_xlabel("X axis")
        self.ax.set_ylabel("Y axis")
        plt.grid(True)
        plt.gca().invert_yaxis()

    def animate(self,i):
        time.sleep(1)

        self.g.set_data(self.list_x[0], self.list_y[0])
        self.r.set_data(self.list_x[1], self.list_y[1])
        return self.g, self.r,

    def run_plot(self):
        anim = animation.FuncAnimation(self.fig, self.animate, frames=200, interval=20)
        plt.show()


class RunMqtt(object):
    """This class runs the MQTT protocol by subscribing to the topic on which data from WSN is published by
     Raspberry-Pi.

     Arguments
     ---------
     list_x : a common list shared by two processes. recmsg method from this class adds new values to the list
              and PlotPosition class use it to localize objects within environment.
     list_y : this is same as list_x, list_x handles x parameters and list_y handles y parameters
     timestamp_list: list shared by two processes containing timestamps
     x_sensor_data_dict: dictionary shared by two processes representing output of sensors mounted on x axis
     y_sensor_data_dict: dictionary shared by two processes representing output of sensors mounted on y axis

     Methods
     ---------
     subscribe : subscribe to the channel on public broker
     decide_pos : decides the position of an object within environment
     write_to_csv: write results to CSV file
     recmsg: receive message from Raspberry pi and decode it
     mqttDisconnect: raise an error when connection to broker is broken
     run_mqtt : set up client and get data forever

     """
    def __init__(self, list_x, list_y, timestamp_list, x_sensor_data_dict, y_sensor_data_dict):
        self.list_x = list_x
        self.list_y = list_y
        self.timestamp_list = timestamp_list
        self.x_sensor_data_dict = x_sensor_data_dict
        self.y_sensor_data_dict = y_sensor_data_dict

    def subscribe(self, client, data, mid, rc):
        """This method subscribes to the topic on which Raspberry Pi is publishing data data"""
        print("receiving sensor data.  MQTT Subscribe return code: " + str(rc))
        client.subscribe("esrl/data")
        print("MQTT broker connection established with return code: " + str(rc))

    def decide_pos(self, a, b):
        """This method takes x and y sensor arrays as input and returns an array specifying location of object

        Arguments
        -----------
        a : list of output of the senors mounted along y axis
        b: list of output of the senors mounted along x axis

         Returns
         --------
         plot_green_x : x co-ordinate array of person sitting
         plot_green_y : y co-ordinate array of person sitting
         plot_red_x : x co-ordinate array of person standing
         plot_red_y: y co-ordinate array of person standing

         """
        plot_green_x = []
        plot_green_y = []
        plot_red_x = []
        plot_red_y = []
        ignore = 0
        for i in range(len(a)):
            if ignore == i:
                continue
            else:
                if a[i] < 250:
                    y = ((250 / 9) * (i + 1))
                    x = a[i]
                    x_point = int(a[i] / 28)
                    before = x_point - 1
                    if b[x_point - 1] < 180 or b[before - 1] < 180:
                        plot_red_x.append(x)
                        plot_red_y.append(y)
                    else:
                        plot_green_x.append(x)
                        plot_green_y.append(y)
                    ignore = i + 1
        return plot_green_x, plot_green_y, plot_red_x, plot_red_y

    def write_to_csv(self, dict_data):
        """This method writes sensor data to CSV file

        Arguments
        ---------
        dict_data: dictionary with keys as column name

        """
        global write_header
        f = open("./"+data_class+".csv", "a", newline="")
        writer = csv.DictWriter(f, fieldnames=csv_columns)
        if write_header:
            writer.writeheader()
        writer.writerow(dict_data)
        write_header = False

    def recmsg(self, client, data, msg):
        """This method decodes received data and update shared lists and dictionary"""

        global allData
        global xpiReceived
        global ypiReceived
        global x_keys
        global y_keys
        global x_p
        global y_p

        jobject = msg.payload.decode()
        rmsg = json.loads(jobject)
        do_not_convert = ["timestamp", "origin", "piezoString"]

        if rmsg['origin'] == "XPi":
            xpiReceived = 1

            if xpiReceived == 1 and ypiReceived == 0:
                allData.clear()

            pz_list = rmsg["piezoString"].split(",")
            allData["pz0"] = float(pz_list[0])
            allData["pz1"] = float(pz_list[1])
            allData["pz2"] = float(pz_list[2])
            allData["pz3"] = float(pz_list[3])

            allData.update(dict((k, round(float(v), 2)) for k, v in rmsg.items() if k not in do_not_convert))
            allData["timestamp"] = rmsg["timestamp"]

            allData.pop("piezoString", None)

            xpiReceived = 0

        elif rmsg["origin"] == "YPi":
            ypiReceived = 1
            if ypiReceived == 1:
                rmsg.pop("timestampy", None)
                allData.update(dict((k, round(float(v), 2)) for k, v in rmsg.items() if k not in do_not_convert))

            ypiReceived = 0

            if len(allData) > 14:
                self.write_to_csv(allData)

            if len(plot_list) < 5:
                plot_list.append(allData.copy())
                if len(plot_list[0]) < 15:
                    plot_list.pop(0)
            else:
                plot_list.pop(0)
                plot_list.append(allData.copy())

            df = pd.DataFrame(plot_list)

            if not df.empty:

                print(plot_list[-1])

                x_new = [df.iloc[-1][x] for x in x_keys]
                y_new = [df.iloc[-1][y] for y in y_keys]

                ts = df.iloc[-1]["timestamp"]

                green_x, green_y, red_x, red_y = self.decide_pos(y_new, x_new)
                if len(self.list_x) < 1:
                    self.list_x.append(green_x)
                    self.list_x.append(red_x)
                    self.list_y.append(green_y)
                    self.list_y.append(red_y)

                else:
                    self.list_x[0] = green_x
                    self.list_x[1] = red_x
                    self.list_y[0] = green_y
                    self.list_y[1] = red_y

                if len(self.timestamp_list) < 5:
                    self.timestamp_list.append(ts.split()[1])
                else:
                    self.timestamp_list.pop(0)
                    self.timestamp_list.append(ts.split()[1])

                if len(df.index) == 5:
                    for k in range(9):
                        self.x_sensor_data_dict[x_keys[k]] = df.iloc[-5:][x_keys[k]].tolist()
                    for m in range(8):
                        self.y_sensor_data_dict[y_keys[m]] = df.iloc[-5:][y_keys[m]].tolist()

    def mqttDisconn(self, client, data, rc):
        """This method checks for network connectivity with MQTT broker"""
        while True:
            print("disconnect from MQTT triggered. Entering infinite loop. "
                  "Write self-restarting code here if this ever triggers in testing!")
            print("disconnect return code was: " + str(rc))
            time.sleep(2)

    def run_mqtt(self):
        """This method creates client to connect with broker and is separate process in this application """
        client = mqtt.Client()
        client.on_connect = self.subscribe
        client.on_message = self.recmsg
        print("Attempting to connect to MQTT broker")
        client.connect(mqttHost, 1883, 60)
        client.on_disconnect = self.mqttDisconn
        client.loop_forever()


if __name__ == "__main__":
    """Since processes does not have a common memory space like threads, python multiprocessing manager is being used
    to manage shared resources among three processes.
    
    """
    mgr = multiprocessing.Manager()
    list_x_pos = mgr.list()                      # empty list
    list_y_pos = mgr.list()
    timestamp = mgr.list()
    x_sensor_data_dict = mgr.dict()              # empty dictionary
    y_sensor_data_dict = mgr.dict()

    # create objects of all three classes
    plot_object = PlotPosition(list_x=list_x_pos, list_y=list_y_pos)
    mqtt_object = RunMqtt(list_x=list_x_pos, list_y=list_y_pos, timestamp_list=timestamp,
                          x_sensor_data_dict=x_sensor_data_dict, y_sensor_data_dict=y_sensor_data_dict)
    plot_data_object = PlotSensorData(timestamp_list=timestamp, x_sensor_data_dict=x_sensor_data_dict,
                                      y_sensor_data_dict=y_sensor_data_dict)

    # three processes are created with corresponding target functions from three different classes
    p_1 = multiprocessing.Process(target=mqtt_object.run_mqtt)
    p_2 = multiprocessing.Process(target=plot_object.run_plot)
    p_3 = multiprocessing.Process(target=plot_data_object.run_data_plotter)

    p_1.start()
    time.sleep(20)                         # Wait for enough data points to arrive to plot
    p_2.start()
    time.sleep(6)
    p_3.start()
    p_1.join()                            # Join all processes so that main program does not exit until all are executed
    p_2.join()
    p_3.join()
