#!/usr/bin/env python
# coding: utf-8


from mpl_toolkits.axes_grid1 import make_axes_locatable
from datetime import datetime, timedelta
from traffic.core import Traffic
from traffic.data import opensky
from glob import glob

import matplotlib.pyplot as plt
import multiprocessing as mp
import cartopy.crs as ccrs
import numpy as np

# Set output directory for traffic data
odir = ''

# Setting up the start and end times for the retrieval
start_dt = datetime(2020, 4, 6, 0, 0)
end_dt = datetime.utcnow() - timedelta(hours=5)

# Sets the number of simultaneous retrievals
nummer = 6

# Lon0, Lat0, Lon1, Lat1
bounds = [-4, 50.4, 2., 52.4]


def getter(init_time, bounds, timer, outdir, anam):
    """
    This function downloads the data, which is done in
    one hour segments. Each hour is downloaded separately
    using multiprocessing for efficiency.
    """
    try:
        times = init_time + timedelta(hours=timer)
        dtst = times.strftime("%Y%m%d%H%M")
        outf = outdir+'OS_'+dtst+'_'+anam+'.pkl'

        # Check if the file has already been retrieved
        if (os.path.exists(outf)):
            print("Already retrieved", outf)
            return
        # Otherwise use 'traffic' to download
        flights = opensky.history(start=times,
                                  stop=times+timedelta(hours=1),
                                  bounds=bounds,
                                  other_params=" and time-lastcontact<=15 ")
        flights.to_pickle(outf)
    except Exception as e:
        print("Problem with this date/time:", e, times)

    return


while True:
    dtst = start_dt.strftime("%Y%m%d%H%M")
    print("Now processing:", start_dt.strftime("%Y/%m/%d %H:%M"))

    # Create processes for each hour, in total 'nummer' hours are
    # processed simultaneously
    processes = [mp.Process(target=getter,
                            args=(start_dt, bounds, i, odir, 'EUR'))
                 for i in range(0, nummer)]

    # Start, and then join, all processes
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    # Move on to the next block of times
    start_dt = start_dt + timedelta(hours=nummer)

    # If we have reached the end of the block then exit
    if (start_dt >= end_dt):
        break


files = glob(odir + '*.pkl')
files.sort()
first = True
for inf in files:
    if first:
        fdata = Traffic.from_file(inf).query("latitude == latitude")
        fdata = fdata.clean_invalid().filter().eval()
        first = False
    else:
        tdata = Traffic.from_file(inf).query("latitude == latitude")
        tdata = tdata.clean_invalid().filter().eval()
        fdata = fdata + tdata
    print(len(fdata))


figsize = (20, 20)
projparm = ccrs.PlateCarree(central_longitude=(bounds[2] - bounds[0])/2)

alt_cut = 500

fig, ax = plt.subplots(1, 1, figsize=figsize,
                       subplot_kw=dict(projection=projparm))
ax = plt.axes(projection=ccrs.PlateCarree())

divider = make_axes_locatable(ax)
ax_cb = divider.new_horizontal(size="5%", pad=0.1, axes_class=plt.Axes)

ax.set_xlim(bounds[0], bounds[2])
ax.set_ylim(bounds[1], bounds[3])

ax.background_patch.set_facecolor('black')
ax.coastlines(resolution='10m', color='white', linewidth=0.8)

count = 0
count2 = 0
n_fl = len(fdata)

for flight in fdata:
    f2 = flight.resample('10s')
    alts = np.array(f2.data['altitude'])
    if np.all(alts < 8000):
        continue
    if np.nanmean(alts[0:5]) < 5000 and np.nanmean(alts[-6:-1]) > 3000:
        colfl = '#35618f'
    elif np.nanmean(alts[-6:-1]) < 3000 and np.nanmean(alts[0:5]) > 3000:
        colfl = '#9ae871'
    else:
        colfl = '#7c3eba'
    plt.plot(flight.data['longitude'], flight.data['latitude'],
             linewidth=0.4, c=colfl)

    # These print some things so we can ensure it's not stuck,
    # as occasionally happens when running a notebook over a VPN.
    count += 1
    count2 += 1
    if count >= 50:
        print(count2, n_fl)
        count = 0

# These are some fake lines used for the legend
plt.plot([-10, -10], [-10, -10], c='#35618f', label='Take-Off')
plt.plot([-10, -10], [-10, -10], c='#9ae871', label='Landing')
plt.plot([-10, -10], [-10, -10], c='#7c3eba', label='Overflight')

ax.legend(fontsize=14, fancybox=True, framealpha=0.99)
plt.savefig('./ENGLAND_FLIGHTS_MAIN.svg', bbox_inches='tight',
            pad_inches=0, dpi=600)
