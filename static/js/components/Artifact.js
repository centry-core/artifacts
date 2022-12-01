const Artifact = {
    props: ['session'],
    components: {
        'artifact-bucket-modal': ArtifactBucketModal,
        'artifact-bucket-update-modal': ArtifactBucketUpdateModal,
        'artifact-files-table': ArtifactFilesTable,
        'artifact-bucket-aside': ArtifactBucketAside,
        'artifact-confirm-modal': ArtifactConfirmModal,
    },
    data() {
        return {
            selectedBucket: {
                name: null,
            },
            selectedBucketRowIndex: null,
            loadingDelete: false,
            isInitDataFetched: false,
            showConfirm: false,
            bucketCount: 0,
        }
    },
    mounted() {
        const vm = this;
        this.fetchBuckets().then(data => {
            $("#bucket-table").bootstrapTable('append', data.rows);
            this.setBucketEvent(data.rows)
            this.bucketCount = data.rows.length;
            this.isInitDataFetched = true;
            if (data.rows.length > 0) {
                this.selectFirstBucket();
            }
            return data.rows
        }).then((rows) => {
            if (rows.length > 0) {
                this.fetchArtifacts(vm.selectedBucket.name).then(data => {
                    $("#artifact-table").bootstrapTable('append', data.rows);
                })
            }
        })
    },
    methods: {
        setBucketEvent(bucketList) {
            const vm = this;
            $('#bucket-table').on('click', 'tbody tr:not(.no-records-found)', function(event) {
                const selectedUniqId = this.getAttribute('data-uniqueid');
                vm.selectedBucket = bucketList.find(row => row.id === selectedUniqId);
                $(this).addClass('highlight').siblings().removeClass('highlight');
                vm.refreshArtifactTable(vm.selectedBucket.name);
                this.tableData = this.taskResults[vm.selectedBucket.name]
            });
        },
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
        refreshBucketTable(bucketId = null) {
            this.fetchBuckets().then(data => {
                $("#bucket-table").bootstrapTable('load', data.rows);
                $('#bucket-table').off('click', 'tbody tr:not(.no-records-found)')
                this.setBucketEvent(data.rows);
                if (bucketId) {
                    this.selectedBucket = data.rows.find(row => row.id === bucketId);
                    $('#bucket-table').find(`[data-uniqueid='${bucketId}']`).addClass('highlight');
                } else {
                    this.selectFirstBucket();
                }
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
                    vm.selectedBucket = $('#bucket-table').bootstrapTable('getData')[0];
                }
            })
        },
        refresh(newSize) {
            this.selectedBucket.size = newSize;
            $('#bucket-table').bootstrapTable('updateRow', {
                index: this.selectedBucketRowIndex,
                row: {
                    size: newSize,
                }
            })
            $('#bucket-table').find(`[data-uniqueid='${this.selectedBucket.id}']`).addClass('highlight');
            this.refreshArtifactTable(this.selectedBucket.name, true);
        },
        switcherDeletingBucket() {
            if (this.bucketDeletingType === 'single') {
                this.deleteBucket()
            } else {
                this.deleteSelectedBuckets()
            }
            // this.bucketDeletingType === 'single' ? this.deleteBucket() : this.deleteSelectedBuckets();
        },
        deleteBucket() {
            this.loadingDelete = true;
            fetch(`/api/v1/artifacts/buckets/${getSelectedProjectId()}?name=${this.selectedBucket.name}`, {
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
                :bucket-count="bucketCount"
                :selected-bucket="selectedBucket"
                :selected-bucket-row-index="selectedBucketRowIndex"
                :is-init-data-fetched="isInitDataFetched">
            </artifact-bucket-aside>
            <artifact-files-table
                :selected-bucket="selectedBucket"
                @refresh="refresh">
            </artifact-files-table>
            <artifact-bucket-modal
                @refresh-bucket="refreshBucketTable">
            </artifact-bucket-modal>
            <artifact-bucket-update-modal
                :selected-bucket="selectedBucket">
            </artifact-bucket-update-modal>
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