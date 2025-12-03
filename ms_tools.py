from casacore.tables import table, taql
import numpy as np
from astropy.time import Time
import astropy.units as u
from typing import NamedTuple

class UniqueTimes(NamedTuple):
    """Structure to hold information about times from ms"""

    tsamp: float
    """Sampling time in seconds"""
    nsub: int
    """Number of time samples"""
    times: Time
    """Array of unique times in as astropy Time array in MJD format"""

def get_unique_times_from_ms(ms):
    """Return the unique observation times from an ASKAP Measurement
    set along with the number of integrations and time sampling.

    Useful for making cubes

    Args:
        ms (Union[MS, Path]): Measurement set to inspect

    Returns:
        Time: The observation times
    """
    
    with table(ms, ack=False) as tab:
        times = Time(np.unique(tab.getcol("TIME")) * u.s, format="mjd")

    nsub = len(times)
    tsamp = np.median(np.diff(times)).sec.item()
    return UniqueTimes(tsamp, nsub, times)

def get_fast_imaging_intervals(ms, timestep):
    """
    Get number of time-intervals from a measurement set for a given imaging timestep
    Convenience function for wsclean -intervals-out
    Args:
        ms (MS): The subject measurement set to rename
        timestep: float (in seconds) for doing short-imaging over
    Returns:
        intervals_out (int): the number of intervals to pass to wsclean
    """
    UniqueTime_ = get_unique_times_from_ms(ms)
    interval, nsub, times = UniqueTime_  # int, float, Time
    duration = (times.max() - times.min()).sec.item()  # sec

    if timestep is None:
        intervals_out = nsub
    else:
        intervals_out = np.max([1, np.round(duration / timestep).astype(int)])
    real_timestep = duration / intervals_out
    print(f"Got {intervals_out} intervals for {timestep} s")
    print(f"Effective timestep is {real_timestep} s")

    return intervals_out

if __name__ == "__main__":

    import sys
    ms = sys.argv[1]
    timestep = float(sys.argv[2])
    intervals = get_fast_imaging_intervals(ms, timestep)
