DATA_DIR := data/
MOUNT_POINT := hsm/

all: rmdirs mkdirs
	fusehsman data/ hsm/

mkdirs:
	mkdir $(DATA_DIR) $(MOUNT_POINT)

rmdirs:
	rm -rf $(DATA_DIR) $(MOUNT_POINT)

develop:
	rm -rf env/
	virtualenv env -p python2.7
	env/bin/pip install -Ue .

