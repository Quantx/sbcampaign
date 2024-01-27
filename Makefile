linux: campaign.c
	gcc -o campaign campaign.c -g -lm -lsystemd `mysql_config --cflags --libs`
