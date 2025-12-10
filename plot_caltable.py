#!/usr/bin/env python3

import casaconfig
casaconfig.logfile = "/dev/null"

def plot_solutions(caltable: str, figfile_base: str):
    """
    Plot calibration solutions using plotms (recommended in modern CASA).
    Saves phase-vs-time and amplitude-vs-time PNGs next to your caltable.
    """
    try:
        from casaplotms import plotms  # CASA 6 plotms interface
    except Exception as e:
        # Some packaged CASA expose plotms via casatasks; try that path too
        try:
            from casatasks import plotms  # fallback
        except Exception:
            raise RuntimeError(f"plotms is not available in this CASA build: {e}")

    # 1) Phase vs Time
    phase_png = f"{figfile_base}.phase.png"
    plotms(
        vis=caltable,          # plotms accepts calibration tables as 'vis'
        xaxis="time",
        yaxis="phase",
        coloraxis="baseline",
        showgui=False,
        plotfile=phase_png,
        overwrite=True
    )

    # 2) Amplitude vs Time (optional; comment out if you only want phase)
    amp_png = f"{figfile_base}.amp.png"
    plotms(
        vis=caltable,
        xaxis="time",
        yaxis="amp",
        coloraxis="baseline",
        showgui=False,
        plotfile=amp_png,
        overwrite=True
    )

    print(f"Saved plots:\n  {phase_png}\n  {amp_png}")
