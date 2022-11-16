const Artifact = {
    props: ['session'],
    components: {
        'artifact-bucket-modal': ArtifactBucketModal,
        'artifact-files-table': ArtifactFilesTable,
        'artifact-bucket-aside': ArtifactBucketAside,
        'artifact-confirm-modal': ArtifactConfirmModal,
    },
    data() {
        return {
            selectedBucked: null,
            selectedBucketRowIndex: null,
            loadingDelete: false,
            isInitDataFetched: false,
            showConfirm: false,
        }
    },
    mounted() {
        const vm = this;
        this.fetchBuckets().then(data => {
            $("#bucket-table").bootstrapTable('append', data.rows);
            $('#bucket-table').on('click', 'tbody tr:not(.no-records-found)', function(event) {
                vm.selectedBucketRowIndex = +this.getAttribute('data-index');
                vm.selectedBucked = this.childNodes[vm.getBucketNameCallIndex(this)].innerHTML;
                $(this).addClass('highlight').siblings().removeClass('highlight');
                vm.refreshArtifactTable(vm.selectedBucked);
            });
            this.isInitDataFetched = true;
            if (data.rows.length > 0) {
                this.selectFirstBucket();
            }
            return data.rows
        }).then((rows) => {
            if (rows.length > 0) {
                this.fetchArtifacts(vm.selectedBucked).then(data => {
                    $("#artifact-table").bootstrapTable('append', data.rows);
                })
            }
        })
    },
    methods: {
        async fetchArtifacts(bucket) {
            const res = await fetch(`/api/v1/artifacts/artifacts/${getSelectedProjectId()}/${bucket}`, {
                method: 'GET',
            })
            return res.json();
        },
        async fetchBuckets() {
            // TODO rewrite session
            const res = await fetch (`/api/v1/artifacts/buckets/${this.session}`,{
                method: 'GET',
            })
            return res.json();
        },
        refreshArtifactTable(bucked) {
            this.fetchArtifacts(bucked).then(data => {
                $("#artifact-table").bootstrapTable('load', data.rows);
            })
        },
        refreshBucketTable() {
            this.fetchBuckets().then(data => {
                $("#bucket-table").bootstrapTable('load', data.rows);
            })
        },
        getBucketNameCallIndex(row) {
            let bucketNameCallIndex;
            row.childNodes.forEach((node, index) => {
                const isBucketNameCell = node.className.split(' ').includes('bucket-name');
                if (isBucketNameCell) {
                    bucketNameCallIndex = index;
                }
            })
            return bucketNameCallIndex;
        },
        selectFirstBucket() {
            const vm = this;
            $('#bucket-table tbody tr').each(function(i, item) {
                if(i === 0) {
                    const firstRow = $(item);
                    firstRow.addClass('highlight');
                    vm.selectedBucketRowIndex = 0;
                    vm.selectedBucked = this.childNodes[vm.getBucketNameCallIndex(this)].innerHTML;
                }
            })
        },
        refresh() {
            this.refreshBucketTable();
            this.refreshArtifactTable(this.selectedBucked, true);
        },
        switcherDeletingBucket() {
            if (this.bucketDeletingType === 'single') {
                console.log('single')
                this.deleteBucket()
            } else {
                console.log('multy')
                this.deleteSelectedBuckets()
            }
            // this.bucketDeletingType === 'single' ? this.deleteBucket() : this.deleteSelectedBuckets();
        },
        deleteBucket() {
            this.loadingDelete = true;
            fetch(`/api/v1/artifacts/buckets/${getSelectedProjectId()}?name=${this.selectedBucked}`, {
                method: 'DELETE',
            }).then((data) => {
                this.refreshBucketTable();
            }).finally(() => {
                this.loadingDelete = false;
                this.showConfirm = !this.showConfirm;
                showNotify('SUCCESS', 'Bucket delete.');
            })
        },
        deleteSelectedBuckets() {
            const selectedBucketList = $("#bucket-table").bootstrapTable('getSelections')
                .map(bucket => bucket.name.toLowerCase());
            const urls = selectedBucketList.map(name => `/api/v1/artifacts/buckets/${getSelectedProjectId()}?name=${name}`)
            this.loadingDelete = true;
            Promise.all([urls.map(url => {
                return fetch(url, {
                    method: 'DELETE',
                })
            })]).then(() => {
                this.refreshBucketTable();
            }).finally(() => {
                this.loadingDelete = false;
                this.showConfirm = !this.showConfirm;
                showNotify('SUCCESS', 'Buckets delete.');
            })
        },
        openConfirm(type) {
            this.bucketDeletingType = type;
            this.showConfirm = !this.showConfirm;
        },
    },
    template: ` 
        <main class="d-flex align-items-start justify-content-center mb-3">
            <artifact-bucket-aside
                @open-confirm="openConfirm"
                :selected-bucket-row-index="selectedBucketRowIndex"
                :is-init-data-fetched="isInitDataFetched">
            </artifact-bucket-aside>
            <artifact-files-table
                :selected-bucked="selectedBucked"
                @refresh="refresh">
            </artifact-files-table>
            <artifact-bucket-modal
                @refresh-bucket="refreshBucketTable">
            </artifact-bucket-modal>
            <artifact-confirm-modal
                v-if="showConfirm"
                @close-confirm="openConfirm"
                :loading-delete="loadingDelete"
                @delete-bucket="switcherDeletingBucket">
            </artifact-confirm-modal>
        </main>
    `
};

register_component('artifact', Artifact);