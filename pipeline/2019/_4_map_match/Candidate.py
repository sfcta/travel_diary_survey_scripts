import map_matching as mm

# Subclass the native Candidate class to support more attributes
class Candidate(mm.Candidate):
    def __init__(self, measurement, edge, location, distance):
        super(Candidate, self).__init__(measurement=measurement, edge=edge, location=location, distance=distance)
        self.lon = None
        self.lat = None