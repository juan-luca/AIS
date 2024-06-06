using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class TyDireccion
    {
        public string DTORIGEN;
        public string DTDEPTO;
        public string DTOBSERVACIONES;
        public string DTVALIDO;
        public string DTDEFECTO;
        public string DTFECBAJA;
        public string DTLOCALIDAD;
        public string DTCODDOM;
        public string DTCOD;
        public string DTCALLE;
        public string DTPROVINCIA;
        public string DTCODPOS;
        public string DTPAIS;
        public string DTNUMERO;
        public string DTPISO;
        public string DTTIPDOM;
        public string DTUSRMODIF;
    }

    class DireccionesDalc : cBase
    {
        public DireccionesDalc(cConexion pconn)
        {
            conn = pconn;
        }

        #region Caraga Unitaria

        internal AISDataReader AbrirCursorNuevosDirecciones()
        {
            String sSql;
            AISDataReader Dr = new AISDataReader();

            sSql = $@" SELECT substr(DTCOD, 1,3) as DTORIGEN, DTCOD, substr(DTCODDOM, 4, length(DTCODDOM)-1)as DTCODDOM, DTTIPDOM, DTCALLE, 
    DTNUMERO, DTPISO, DTDEPTO, DTLOCALIDAD, DTPROVINCIA, DTCODPOS, DTPAIS, DTDEFAULT,DTCOMPDIR, DTFECBAJA
FROM IN_DIRE
WHERE DTFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND DTFECBAJA is null
ORDER BY DTCOD ";

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;
            return Dr;
        }

        internal bool FechDireccion(AISDataReader Dr, ref TyDireccion Direccion)
        {
            try
            {
                Direccion = new TyDireccion();
                if (Dr.Read())
                {
                    Direccion.DTORIGEN = Dr["DTORIGEN"].ToString();
                    Direccion.DTCOD = Dr["DTCOD"].ToString();
                    Direccion.DTCODDOM = Dr["DTCODDOM"].ToString();
                    Direccion.DTTIPDOM = Dr["DTTIPDOM"].ToString();
                    Direccion.DTCALLE = Dr["DTCALLE"].ToString();
                    Direccion.DTNUMERO = Dr["DTNUMERO"].ToString();
                    Direccion.DTPISO = Dr["DTPISO"].ToString();
                    Direccion.DTDEPTO = Dr["DTDEPTO"].ToString();
                    Direccion.DTLOCALIDAD = Dr["DTLOCALIDAD"].ToString();
                    Direccion.DTPROVINCIA = Dr["DTPROVINCIA"].ToString();
                    Direccion.DTCODPOS = Dr["DTCODPOS"].ToString();
                    Direccion.DTPAIS = Dr["DTPAIS"].ToString();
                    Direccion.DTDEFECTO = Dr["DTDEFAULT"].ToString() == "" ? "N" : Dr["DTDEFAULT"].ToString();
                    Direccion.DTOBSERVACIONES = Dr["DTCOMPDIR"].ToString();
                    Direccion.DTFECBAJA = Dr["DTFECBAJA"].ToString();
                    Direccion.DTVALIDO = Dr["DTFECBAJA"].ToString() == "" ? "S" : "N";

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechDireccion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHDIRE", "Error al recuperar Direccion");
                return false;
            }
        }

        internal TipoAccion ObtengoDireccionBD(TyDireccion BDDireccion)
        {
            String sSql;

            DataSet Ds = new DataSet();

            sSql = $@" SELECT DTORIGEN, DTDEPTO,DTOBSERVACIONES, DTVALIDO, DTDEFECTO, DTFECBAJA, DTLOCALIDAD, DTCODDOM, DTCOD, DTCALLE, 
DTPROVINCIA, DTCODPOS, DTPAIS, DTNUMERO, DTPISO, DTTIPDOM, DTUSRMODIF
FROM RDIRE 
WHERE DTORIGEN = {cFormat.StToBD(BDDireccion.DTORIGEN)}
        AND DTCOD = {cFormat.StToBD(BDDireccion.DTCOD)}
        AND DTTIPDOM = {cFormat.StToBD(BDDireccion.DTTIPDOM)}
        AND DTCODDOM = {cFormat.StToBD(BDDireccion.DTCODDOM)} ";

            Ds = conn.EjecutarQuery(sSql, "RDIRE");

            if (conn.Errores.Cantidad() == 0)
            {
                if (Ds.Tables["RDIRE"].Rows.Count > 0)
                {
                    DataRow Dr = Ds.Tables["RDIRE"].Rows[0];

                    BDDireccion.DTORIGEN = Dr["DTORIGEN"].ToString();
                    BDDireccion.DTCOD = Dr["DTCOD"].ToString();
                    BDDireccion.DTCODDOM = Dr["DTCODDOM"].ToString();
                    BDDireccion.DTTIPDOM = Dr["DTTIPDOM"].ToString();
                    BDDireccion.DTCALLE = Dr["DTCALLE"].ToString();
                    BDDireccion.DTNUMERO = Dr["DTNUMERO"].ToString();
                    BDDireccion.DTPISO = Dr["DTPISO"].ToString();
                    BDDireccion.DTDEPTO = Dr["DTDEPTO"].ToString();
                    BDDireccion.DTLOCALIDAD = Dr["DTLOCALIDAD"].ToString();
                    BDDireccion.DTPROVINCIA = Dr["DTPROVINCIA"].ToString();
                    BDDireccion.DTCODPOS = Dr["DTCODPOS"].ToString();
                    BDDireccion.DTPAIS = Dr["DTPAIS"].ToString();
                    BDDireccion.DTDEFECTO = Dr["DTDEFECTO"].ToString();
                    BDDireccion.DTOBSERVACIONES = Dr["DTOBSERVACIONES"].ToString();
                    BDDireccion.DTFECBAJA = Dr["DTFECBAJA"].ToString();
                    BDDireccion.DTVALIDO = Dr["DTFECBAJA"].ToString();
                    BDDireccion.DTUSRMODIF = Dr["DTUSRMODIF"].ToString();

                    return TipoAccion.Modificacion;
                }
                else
                    return TipoAccion.Alta;
            }
            else
            {
                cIncidencia.Generar(conn.Errores, "SELDIRE", "Error en Select de Direcciones. Cliente: " + BDDireccion.DTCOD + " - Codigo Direccion: " + BDDireccion.DTCODDOM + " - Tipo Direccion: " + BDDireccion.DTTIPDOM);
                return TipoAccion.Error;
            }
        }

        internal bool InsertaDireccionBD(TyDireccion Direccion)
        {

            String sSql;
            int nRet;

            sSql = $@" INSERT INTO RDIRE ( DTORIGEN, DTDEPTO,DTOBSERVACIONES, DTVALIDO, DTDEFECTO, DTFECBAJA, DTLOCALIDAD, DTCODDOM, DTCOD, DTCALLE, DTPROVINCIA, DTCODPOS, DTPAIS, DTNUMERO, DTPISO, DTTIPDOM)
values ( 
{cFormat.StToBD(Direccion.DTORIGEN)}, {cFormat.StToBD(Direccion.DTDEPTO)}, {cFormat.StToBD(Direccion.DTOBSERVACIONES)}, {cFormat.StToBD(Direccion.DTVALIDO)},
{cFormat.StToBD(Direccion.DTDEFECTO)}, {cFormat.StToBD(Direccion.DTFECBAJA)}, {cFormat.StToBD(Direccion.DTLOCALIDAD)}, {cFormat.StToBD(Direccion.DTCODDOM)},
{cFormat.StToBD(Direccion.DTCOD)}, {cFormat.StToBD(Direccion.DTCALLE)}, {cFormat.StToBD(Direccion.DTPROVINCIA)}, {cFormat.StToBD(Direccion.DTCODPOS)},
{cFormat.StToBD(Direccion.DTPAIS)}, {cFormat.StToBD(Direccion.DTNUMERO)}, {cFormat.StToBD(Direccion.DTPISO)}, {cFormat.StToBD(Direccion.DTTIPDOM)}
)";
            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaDireccionBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSDIRE", "Error al insertar Direccion. Cliente: " + Direccion.DTCOD + " - Codigo Direccion: " + Direccion.DTCODDOM + " - Tipo Direccion: " + Direccion.DTTIPDOM);
                return false;
            }
        }

        internal bool ModificaDireccionBD(TyDireccion Direccion)
        {

            int nRet;

            string sSql = $@" UPDATE RDIRE SET  
     DTDEPTO = {cFormat.StToBD(Direccion.DTDEPTO)},
     DTOBSERVACIONES = {cFormat.StToBD(Direccion.DTOBSERVACIONES)}, 
     DTVALIDO = {cFormat.StToBD(Direccion.DTVALIDO)}, 
     DTDEFECTO = {cFormat.StToBD(Direccion.DTDEFECTO)}, 
     DTFECBAJA = {cFormat.StToBD(Direccion.DTFECBAJA)}, 
     DTLOCALIDAD = {cFormat.StToBD(Direccion.DTLOCALIDAD)}, 
     DTCALLE = {cFormat.StToBD(Direccion.DTCALLE)}, 
     DTPROVINCIA = {cFormat.StToBD(Direccion.DTPROVINCIA)}, 
     DTCODPOS = {cFormat.StToBD(Direccion.DTCODPOS)}, 
     DTPAIS = {cFormat.StToBD(Direccion.DTPAIS)}, 
     DTNUMERO = {cFormat.StToBD(Direccion.DTNUMERO)}, 
     DTPISO = {cFormat.StToBD(Direccion.DTPISO)}
WHERE DTORIGEN = {cFormat.StToBD(Direccion.DTORIGEN)}
    AND DTCOD = {cFormat.StToBD(Direccion.DTCOD)}
    AND DTTIPDOM = {cFormat.StToBD(Direccion.DTTIPDOM)}
    AND DTCODDOM = {cFormat.StToBD(Direccion.DTCODDOM)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaDireccionBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDDIRE", "Error al modificar Direccion. Cliente: " + Direccion.DTCOD + " - Codigo Direccion: " + Direccion.DTCODDOM + " - Tipo Direccion: " + Direccion.DTTIPDOM);
                return false;
            }
        }

        internal bool ClienteParaDarDeBajaBD(string sCodClie)
        {

            String sSql;

            DataSet Ds = new DataSet();

            sSql = $@" SELECT COUNT(OCRAIZ) FROM ROBCL
            WHERE OCFECPROC= {cFormat.StToBD(cGlobales.Hoy)}
             AND OCRAIZ = {cFormat.StToBD(sCodClie)} ";

            Ds = conn.EjecutarQuery(sSql, "RCLIEB");

            if (conn.Errores.Cantidad() == 0)
            {
                //if (Convert.ToInt32(Ds.Tables["RCLIEB"].Rows[0][0].ToString()) > 0)
                if (Convert.ToInt32(Ds.Tables["RCLIEB"].Rows[0][0].ToString()) == 0)
                    return true;
                else
                    return false;

            }
            else
            {
                cIncidencia.Generar(conn.Errores, "SELCLIE", "Error al verificar baja cliente. Cliente: " + sCodClie);
                return false;
            }


        }
        
        internal bool MarcoComoInvalidoLaDireccionBD(TyDireccion Direccion)
        {
            String sSql;
            int nRet;

            sSql = " UPDATE RDIRE SET  ";
            sSql += " DTVALIDO	= '0', ";
            sSql += " DTFECMODIF  = NULL, ";
            sSql += " DTHORAMODIF  = NULL, ";
            sSql += " DTUSRMODIF = NULL ";
            sSql += " WHERE DTCOD = " + cFormat.StToBD(Direccion.DTCOD) + " ";
            sSql += "   AND DTCODDOM = " + cFormat.StToBD(Direccion.DTCODDOM) + " ";
            sSql += "   AND DTTIPDOM = " + cFormat.StToBD(Direccion.DTTIPDOM) + " ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "MarcoComoInvalidoLaDireccionBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "MINVDIRE", "Error al invalidar Direccion. Cliente: " + Direccion.DTCOD + " - Codigo Direccion: " + Direccion.DTCODDOM + " - Tipo Direccion: " + Direccion.DTTIPDOM);
                return false;
            }
        }

        #endregion

        #region Carga Masiva

        internal void BorrarDirecciones()
        {
            string sSql = $@" DELETE FROM RDIRE P
                            WHERE UPPER(DTORIGEN) IN ('COM','LSG') 
                            AND EXISTS (SELECT 1 FROM IN_DIRE I WHERE I.DTFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND I.DTCOD = P.DTCOD AND I.DTTIPDOM = P.DTTIPDOM) ";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
        }

        internal void InsertareDirecciones()
        {
            string sSql = $@"INSERT INTO RDIRE (DTCOD, DTCODDOM, DTTIPDOM, DTCALLE, DTNUMERO, DTPISO, DTDEPTO, DTLOCALIDAD, DTPROVINCIA, DTCODPOS, DTPAIS, DTDEFECTO, DTOBSERVACIONES, DTORIGEN)
SELECT DTCOD, DTCODDOM, DTTIPDOM, DTCALLE, DTNUMERO, DTPISO, DTDEPTO, DTLOCALIDAD, DTPROVINCIA, DTCODPOS, DTPAIS, DTDEFAULT, DTCOMPDIR, SUBSTR(DTCOD, 1, 3) DTORIGEN
FROM IN_DIRE
WHERE DTFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = DTCOD) ";

            int res = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
            else
            {
                cIncidencia.Aviso($"Direcciones tratadas: {res}");
            }
        }

        #endregion

    }
}
