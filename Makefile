myo-rsync-dry-run:
	rsync -n -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t myo" ./ :project/muscope/muscope-data-loaders

myo-rsync:
	rsync -arvzP --delete --exclude-from=rsync.exclude -e "ssh -A -t hpc ssh -A -t myo" ./ :project/muscope/muscope-data-loaders
