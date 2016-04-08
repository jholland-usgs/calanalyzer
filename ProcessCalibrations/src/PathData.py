class PathData(object):

    def __init__(self, cal_id, network, station,
                 location, date, channel, cal_duration, ps):
        self.cal_id = cal_id
        self.network = network
        self.station = station
        self.location = location
        self.date = date
        self.channel = channel
        self.cal_duration = cal_duration
        self.ps = ps