import glob
import os

uvfitsfiles=sorted(glob.glob("/fred/oz451/azic/data/SB77974/*/*.uvfits"))
print(len(uvfitsfiles))
for uvfile in uvfitsfiles:
    if not os.path.exists(uvfile.replace(".uvfits", ".ms")):
        importuvfits(uvfile, vis=uvfile.replace(".uvfits", ".ms"))
